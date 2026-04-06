#!/usr/bin/env python3
"""
Realestate.co.nz scraper — Queenstown Lakes District residential listings.
Uses the platform search API directly (no headless browser required).

Covers:
  - District 300  (Queenstown)
  - District 301  (Wanaka)

Price filter strategy:
  - Fixed-price listings (price-code 1, 8, 16): included only if price >= $2M
  - Auction / Deadline Sale / Tender / Negotiation / POA: ALWAYS included.
    In the QLDC luxury market these are typically $2M+ properties.

Tenure is mapped from the API's title-type field:
  1 = Freehold, 2 = Cross-lease, 3 = Unit Title, 4 = Leasehold

Body corporate levy: scraped from the individual listing page for Unit Title
properties (title-type == 3).

New in this version:
  - Snapshot table: records key fields after every run, tagged with run date.
  - Diff engine: compares latest two snapshots and flags what changed.
  - Plain-English weekly summary: saved as a .txt file alongside the database.

Usage:
    python3 scraper.py

Requirements (install once):
    pip3 install requests beautifulsoup4 lxml
"""

import re
import sqlite3
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_BASE    = "https://platform.realestate.co.nz/search/v1/listings"
SITE_BASE   = "https://www.realestate.co.nz"

DISTRICTS   = [300, 301]   # 300 = Queenstown, 301 = Wanaka
PRICE_MIN   = 2_000_000

PAGE_LIMIT  = 50
DELAY       = 1.5

DB_PATH      = Path(__file__).parent / "listings.db"
SUMMARY_PATH = Path(__file__).parent / "weekly_summary.txt"

# Thresholds for flagging listings in the summary
DOM_AMBER = 60   # days — worth watching
DOM_RED   = 90   # days — vendor likely uncomfortable

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-NZ,en;q=0.9",
    "Referer":         "https://www.realestate.co.nz/",
    "Origin":          "https://www.realestate.co.nz",
}

TENURE_MAP = {
    1: "Freehold",
    2: "Cross-lease",
    3: "Unit Title",
    4: "Leasehold",
    5: "Stratum in Fee",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database — listings table (unchanged) + snapshots table (new)
# ---------------------------------------------------------------------------

def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row

    # Original listings table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id           TEXT    UNIQUE,
            url                  TEXT,
            address              TEXT,
            asking_price         TEXT,
            price_numeric        INTEGER,
            price_method         TEXT,
            agency               TEXT,
            agent                TEXT,
            days_on_market       INTEGER,
            published_date       TEXT,
            tenure               TEXT,
            body_corporate_levy  TEXT,
            bedrooms             INTEGER,
            bathrooms            INTEGER,
            land_area_sqm        REAL,
            floor_area_sqm       REAL,
            scraped_at           TEXT,
            updated_at           TEXT
        )
    """)

    # NEW: Snapshots table — one row per listing per run
    # This is what makes the diff possible.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date       TEXT NOT NULL,        -- ISO date of this run, e.g. 2026-03-18
            listing_id     TEXT NOT NULL,
            address        TEXT,
            price_numeric  INTEGER,
            price_method   TEXT,
            asking_price   TEXT,
            agency         TEXT,
            days_on_market INTEGER,
            tenure         TEXT,
            url            TEXT
        )
    """)

    conn.commit()
    return conn


def upsert(conn: sqlite3.Connection, row: dict):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO listings
            (listing_id, url, address, asking_price, price_numeric, price_method,
             agency, agent, days_on_market, published_date,
             tenure, body_corporate_levy,
             bedrooms, bathrooms, land_area_sqm, floor_area_sqm,
             scraped_at, updated_at)
        VALUES
            (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(listing_id) DO UPDATE SET
            url                  = excluded.url,
            address              = excluded.address,
            asking_price         = excluded.asking_price,
            price_numeric        = excluded.price_numeric,
            price_method         = excluded.price_method,
            agency               = excluded.agency,
            agent                = excluded.agent,
            days_on_market       = excluded.days_on_market,
            published_date       = excluded.published_date,
            tenure               = excluded.tenure,
            body_corporate_levy  = excluded.body_corporate_levy,
            bedrooms             = excluded.bedrooms,
            bathrooms            = excluded.bathrooms,
            land_area_sqm        = excluded.land_area_sqm,
            floor_area_sqm       = excluded.floor_area_sqm,
            updated_at           = excluded.updated_at
        """,
        (
            row["listing_id"], row["url"], row["address"],
            row["asking_price"], row["price_numeric"], row["price_method"],
            row["agency"], row["agent"],
            row["days_on_market"], row["published_date"],
            row["tenure"], row["body_corporate_levy"],
            row["bedrooms"], row["bathrooms"],
            row["land_area_sqm"], row["floor_area_sqm"],
            now, now,
        ),
    )


def save_snapshot(conn: sqlite3.Connection, run_date: str, row: dict):
    """Record a snapshot of this listing for today's run."""
    conn.execute(
        """
        INSERT INTO snapshots
            (run_date, listing_id, address, price_numeric, price_method,
             asking_price, agency, days_on_market, tenure, url)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            run_date,
            row["listing_id"],
            row["address"],
            row["price_numeric"],
            row["price_method"],
            row["asking_price"],
            row["agency"],
            row["days_on_market"],
            row["tenure"],
            row["url"],
        ),
    )

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_price(price_display: str) -> Optional[int]:
    m = re.search(r"\$\s*([\d,]+)", price_display or "")
    if m:
        return int(m.group(1).replace(",", ""))
    return None


def price_method_label(price_code: Optional[int]) -> str:
    return {
        1:  "Fixed Price",
        2:  "POA",
        3:  "Negotiation",
        4:  "By Negotiation",
        5:  "Auction",
        6:  "Tender",
        7:  "Deadline Sale",
        8:  "Asking Price",
        16: "Offers Over",
    }.get(price_code, f"Code {price_code}" if price_code else "Unknown")


def days_since(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str)
        now = datetime.now(dt.tzinfo or timezone.utc)
        return (now - dt).days
    except ValueError:
        return None


def build_included_index(included: list) -> dict:
    idx = {}
    for item in (included or []):
        key = (item.get("type"), item.get("id"))
        idx[key] = item.get("attributes", {})
    return idx


def fmt_price(price_numeric: Optional[int], asking_price: str, price_method: str) -> str:
    """Format a price for display in the summary."""
    if price_numeric:
        return f"${price_numeric:,.0f} ({price_method})"
    return asking_price or price_method or "Price unknown"

# ---------------------------------------------------------------------------
# Tenure + body corporate levy from detail page
# ---------------------------------------------------------------------------

def get_levy_from_detail(url: str, session: requests.Session) -> str:
    try:
        time.sleep(DELAY)
        resp = session.get(url, headers={**HEADERS, "Accept": "text/html"}, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.warning("Detail fetch failed (%s): %s", url, e)
        return ""

    soup = BeautifulSoup(resp.text, "lxml")
    text = soup.get_text(" ", strip=True)

    bc = re.search(
        r"(?:body\s+corporate\s+(?:levy|fees?|charges?)[^\n$]{0,30}\$[\d,]+"
        r"|\$[\d,]+\s*(?:per\s+annum|p\.?a\.?|pa|annually)\s*(?:body\s+corporate)?"
        r"|levy[:\s]+\$[\d,]+)",
        text,
        re.IGNORECASE,
    )
    return bc.group(0).strip()[:150] if bc else ""

# ---------------------------------------------------------------------------
# API pagination
# ---------------------------------------------------------------------------

def fetch_page(session: requests.Session, offset: int) -> dict:
    params = []
    for did in DISTRICTS:
        params.append(("filter[district][]", did))
    params += [
        ("filter[category][]", "res_sale"),
        ("page[offset]",       offset),
        ("page[limit]",        PAGE_LIMIT),
        ("include",            "offices,agents"),
    ]
    resp = session.get(API_BASE, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()

# ---------------------------------------------------------------------------
# Diff engine — compares the two most recent snapshot runs
# ---------------------------------------------------------------------------

def get_run_dates(conn: sqlite3.Connection) -> list:
    """Return all distinct run dates, most recent first."""
    cur = conn.execute(
        "SELECT DISTINCT run_date FROM snapshots ORDER BY run_date DESC"
    )
    return [r[0] for r in cur.fetchall()]


def get_snapshot(conn: sqlite3.Connection, run_date: str) -> dict:
    """Return a dict of listing_id -> snapshot row for a given run date."""
    cur = conn.execute(
        "SELECT * FROM snapshots WHERE run_date = ?", (run_date,)
    )
    return {r["listing_id"]: dict(r) for r in cur.fetchall()}


def run_diff(conn: sqlite3.Connection) -> dict:
    """
    Compare the two most recent snapshot runs.
    Returns a dict with keys:
      new_listings      — appeared this week
      removed_listings  — gone since last week (sold or withdrawn)
      price_reductions  — price dropped
      price_increases   — price increased (unusual but worth knowing)
      dom_flags         — listings crossing DOM thresholds this run
      current_total     — total active listings this run
      previous_total    — total active listings last run
    """
    dates = get_run_dates(conn)

    if len(dates) < 2:
        return None  # Not enough history yet

    latest_date, previous_date = dates[0], dates[1]
    latest   = get_snapshot(conn, latest_date)
    previous = get_snapshot(conn, previous_date)

    new_listings      = []
    removed_listings  = []
    price_reductions  = []
    price_increases   = []
    dom_flags         = []

    latest_ids   = set(latest.keys())
    previous_ids = set(previous.keys())

    # New this week
    for lid in latest_ids - previous_ids:
        new_listings.append(latest[lid])

    # Gone since last week
    for lid in previous_ids - latest_ids:
        removed_listings.append(previous[lid])

    # Changed listings
    for lid in latest_ids & previous_ids:
        curr = latest[lid]
        prev = previous[lid]

        curr_price = curr.get("price_numeric")
        prev_price = prev.get("price_numeric")

        if curr_price and prev_price and curr_price != prev_price:
            change_pct = ((curr_price - prev_price) / prev_price) * 100
            entry = {**curr, "prev_price": prev_price, "change_pct": change_pct}
            if curr_price < prev_price:
                price_reductions.append(entry)
            else:
                price_increases.append(entry)

        # DOM threshold crossings
        dom = curr.get("days_on_market")
        prev_dom = prev.get("days_on_market")
        if dom is not None and prev_dom is not None:
            if dom >= DOM_RED > prev_dom:
                dom_flags.append({**curr, "threshold": DOM_RED})
            elif dom >= DOM_AMBER > prev_dom:
                dom_flags.append({**curr, "threshold": DOM_AMBER})

    # Sort price reductions by percentage (largest first)
    price_reductions.sort(key=lambda x: x["change_pct"])

    return {
        "latest_date":     latest_date,
        "previous_date":   previous_date,
        "new_listings":    new_listings,
        "removed_listings":removed_listings,
        "price_reductions":price_reductions,
        "price_increases": price_increases,
        "dom_flags":       dom_flags,
        "current_total":   len(latest),
        "previous_total":  len(previous),
    }

# ---------------------------------------------------------------------------
# Freehold count helper
# ---------------------------------------------------------------------------

def freehold_count(conn: sqlite3.Connection, run_date: str) -> int:
    cur = conn.execute(
        "SELECT COUNT(*) FROM snapshots WHERE run_date = ? AND tenure = 'Freehold'",
        (run_date,),
    )
    return cur.fetchone()[0]

# ---------------------------------------------------------------------------
# Plain-English summary generator
# ---------------------------------------------------------------------------

def generate_summary(conn: sqlite3.Connection, run_date: str) -> str:
    diff = run_diff(conn)
    lines = []

    lines.append("=" * 60)
    lines.append("KAWARAU HEIGHTS — MARKET INTELLIGENCE SUMMARY")
    lines.append(f"Run date: {run_date}")
    lines.append("=" * 60)
    lines.append("")

    if diff is None:
        lines.append("This is the first run. No comparison available yet.")
        lines.append("Run the scraper again next week to generate a diff.")
        lines.append("")

        # Still show current state
        cur_total = conn.execute(
            "SELECT COUNT(*) FROM snapshots WHERE run_date = ?", (run_date,)
        ).fetchone()[0]
        fh = freehold_count(conn, run_date)
        lines.append(f"Active listings captured this run: {cur_total}")
        lines.append(f"Freehold listings: {fh}")
        lines.append("")
        lines.append(f"Database: {DB_PATH}")
        return "\n".join(lines)

    # --- Inventory overview ---
    total_change = diff["current_total"] - diff["previous_total"]
    direction    = "up" if total_change > 0 else "down" if total_change < 0 else "unchanged"
    fh_now       = freehold_count(conn, diff["latest_date"])
    fh_prev      = freehold_count(conn, diff["previous_date"])
    fh_change    = fh_now - fh_prev

    lines.append("INVENTORY")
    lines.append("-" * 40)
    lines.append(
        f"Active listings: {diff['current_total']} "
        f"({direction} from {diff['previous_total']} last week, "
        f"change of {abs(total_change)})."
    )
    lines.append(
        f"Freehold listings: {fh_now} "
        f"({'up' if fh_change > 0 else 'down' if fh_change < 0 else 'unchanged'} "
        f"from {fh_prev} last week)."
    )
    lines.append("")

    # --- New listings ---
    lines.append("NEW LISTINGS THIS WEEK")
    lines.append("-" * 40)
    if diff["new_listings"]:
        for r in diff["new_listings"]:
            price_str = fmt_price(r.get("price_numeric"), r.get("asking_price", ""), r.get("price_method", ""))
            lines.append(
                f"  + {r['address'] or 'Address unknown'} | {price_str} | "
                f"{r.get('agency', 'Agency unknown')} | Tenure: {r.get('tenure', 'unknown')}"
            )
    else:
        lines.append("  No new listings this week.")
    lines.append("")

    # --- Removals ---
    lines.append("REMOVED SINCE LAST WEEK (sold or withdrawn)")
    lines.append("-" * 40)
    if diff["removed_listings"]:
        for r in diff["removed_listings"]:
            price_str = fmt_price(r.get("price_numeric"), r.get("asking_price", ""), r.get("price_method", ""))
            lines.append(
                f"  - {r['address'] or 'Address unknown'} | {price_str} | "
                f"{r.get('agency', 'Agency unknown')} | Was on market {r.get('days_on_market', '?')} days"
            )
    else:
        lines.append("  No listings removed this week.")
    lines.append("")

    # --- Price reductions ---
    lines.append("PRICE REDUCTIONS")
    lines.append("-" * 40)
    if diff["price_reductions"]:
        for r in diff["price_reductions"]:
            curr_p = r.get("price_numeric")
            prev_p = r.get("prev_price")
            pct    = abs(r.get("change_pct", 0))
            lines.append(
                f"  v {r['address'] or 'Address unknown'} | "
                f"${prev_p:,.0f} -> ${curr_p:,.0f} | "
                f"Down {pct:.1f}% | "
                f"{r.get('agency', 'Agency unknown')} | "
                f"DOM: {r.get('days_on_market', '?')} days"
            )
    else:
        lines.append("  No price reductions this week.")
    lines.append("")

    # --- Price increases ---
    if diff["price_increases"]:
        lines.append("PRICE INCREASES")
        lines.append("-" * 40)
        for r in diff["price_increases"]:
            curr_p = r.get("price_numeric")
            prev_p = r.get("prev_price")
            pct    = abs(r.get("change_pct", 0))
            lines.append(
                f"  ^ {r['address'] or 'Address unknown'} | "
                f"${prev_p:,.0f} -> ${curr_p:,.0f} | "
                f"Up {pct:.1f}% | "
                f"{r.get('agency', 'Agency unknown')}"
            )
        lines.append("")

    # --- DOM flags ---
    lines.append("DAYS ON MARKET FLAGS")
    lines.append("-" * 40)
    if diff["dom_flags"]:
        for r in diff["dom_flags"]:
            threshold = r.get("threshold")
            label = f"crossed {threshold} days" if threshold else "threshold crossed"
            lines.append(
                f"  ! {r['address'] or 'Address unknown'} | "
                f"{label} | "
                f"DOM now: {r.get('days_on_market', '?')} | "
                f"{r.get('agency', 'Agency unknown')} | "
                f"Price: {fmt_price(r.get('price_numeric'), r.get('asking_price', ''), r.get('price_method', ''))}"
            )
    else:
        lines.append("  No listings crossing DOM thresholds this week.")
    lines.append("")

    lines.append("=" * 60)
    lines.append(f"Comparison: {diff['previous_date']} vs {diff['latest_date']}")
    lines.append(f"Database: {DB_PATH}")
    lines.append("=" * 60)

    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Main scrape loop
# ---------------------------------------------------------------------------

def scrape(conn: sqlite3.Connection):
    session = requests.Session()
    session.headers.update(HEADERS)

    run_date = datetime.now().strftime("%Y-%m-%d")

    offset = 0
    total_results = None
    saved = 0
    skipped = 0

    while True:
        log.info("Fetching API page: offset=%d", offset)
        data = fetch_page(session, offset)

        meta = data.get("meta", {})
        if total_results is None:
            total_results = meta.get("totalResults", 0)
            log.info("Total listings in QLDC: %d", total_results)

        listings = data.get("data", [])
        if not listings:
            log.info("No more listings.")
            break

        included_idx = build_included_index(data.get("included", []))

        for item in listings:
            attrs = item.get("attributes", {})
            rels  = item.get("relationships", {})

            price_code    = attrs.get("price-code")
            price_display = attrs.get("price-display", "")
            price_numeric = extract_price(price_display)
            price_method  = price_method_label(price_code)

            if price_code == 1 and price_numeric and price_numeric < PRICE_MIN:
                skipped += 1
                continue
            if price_code in (8, 16) and price_numeric and price_numeric < PRICE_MIN:
                skipped += 1
                continue

            listing_id = str(attrs.get("listing-no", item.get("id", "")))
            url        = attrs.get("website-full-url", "")
            addr_obj   = attrs.get("address", {})
            address    = addr_obj.get("full-address", "")

            published_date = attrs.get("published-date") or attrs.get("created-date")
            days_on_market = days_since(published_date)

            title_type = attrs.get("title-type")
            tenure     = TENURE_MAP.get(title_type, "")
            if not tenure and title_type is not None:
                tenure = f"Type {title_type}"

            body_corporate_levy = ""
            if title_type == 3 and url:
                log.info("  Unit Title — fetching levy from: %s", url)
                body_corporate_levy = get_levy_from_detail(url, session)

            agency = ""
            office_rels = rels.get("offices", {}).get("data", [])
            if office_rels:
                office_attrs = included_idx.get(("office", office_rels[0]["id"]), {})
                agency = office_attrs.get("colloquial-name") or office_attrs.get("name", "")

            agent = ""
            agent_rels = rels.get("agents", {}).get("data", [])
            if agent_rels:
                agent_attrs = included_idx.get(("agent", agent_rels[0]["id"]), {})
                agent = agent_attrs.get("name", "")

            bedrooms   = attrs.get("bedroom-count")
            bathrooms  = attrs.get("bathroom-count") or attrs.get("bathrooms-total-count")
            land_area  = attrs.get("land-area")
            floor_area = attrs.get("floor-area")

            row = dict(
                listing_id          = listing_id,
                url                 = url,
                address             = address,
                asking_price        = price_display,
                price_numeric       = price_numeric,
                price_method        = price_method,
                agency              = agency,
                agent               = agent,
                days_on_market      = days_on_market,
                published_date      = published_date,
                tenure              = tenure,
                body_corporate_levy = body_corporate_levy,
                bedrooms            = bedrooms,
                bathrooms           = bathrooms,
                land_area_sqm       = land_area,
                floor_area_sqm      = floor_area,
            )

            upsert(conn, row)
            save_snapshot(conn, run_date, row)   # NEW: snapshot this listing
            conn.commit()
            saved += 1

            log.info(
                "  ✓ %s | %s | %s | DOM: %s days | Tenure: %s",
                address or "(no address)",
                price_display or price_method,
                agency or "(agency unknown)",
                days_on_market if days_on_market is not None else "?",
                tenure or "unknown",
            )

        offset += PAGE_LIMIT
        if offset >= total_results:
            log.info("All pages fetched.")
            break

        time.sleep(DELAY)

    log.info("Saved: %d  Skipped (sub-$2M fixed price): %d", saved, skipped)

    # --- Generate and save summary ---
    log.info("Generating weekly summary...")
    summary = generate_summary(conn, run_date)

    print("\n")
    print(summary)

    with open(SUMMARY_PATH, "w") as f:
        f.write(summary)

    log.info("Summary saved to: %s", SUMMARY_PATH)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Starting scraper -> %s", DB_PATH)
    conn = init_db(DB_PATH)
    try:
        scrape(conn)
    finally:
        conn.close()

    print(f"\n{'='*60}")
    print(f"Database: {DB_PATH}")
    print(f"Summary:  {SUMMARY_PATH}")
    print(f"{'='*60}")
    print("\nUseful queries:")
    print()
    print("  All snapshots by run date:")
    print("    SELECT run_date, COUNT(*) FROM snapshots GROUP BY run_date;")
    print()
    print("  Current listings sorted by price:")
    print("    SELECT address, asking_price, agency, days_on_market, tenure")
    print("    FROM listings ORDER BY price_numeric DESC NULLS LAST;")
    print()
    print("  Freehold listings only:")
    print("    SELECT address, asking_price, agency, days_on_market")
    print("    FROM listings WHERE tenure = 'Freehold'")
    print("    ORDER BY price_numeric DESC NULLS LAST;")
    print()
    print(f"  Open DB: sqlite3 \"{DB_PATH}\"")
