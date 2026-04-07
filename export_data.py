#!/usr/bin/env python3
"""
export_data.py — Kawarau Heights Market Intelligence
======================================================
Reads listings.db (created by scraper.py) and generates listings_data.json
for the Index.html dashboard.

Run this after scraper.py has completed:
    python3 export_data.py

Or use market_launcher.py to do both automatically and open the dashboard.
"""

import json
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

DB_PATH   = Path(__file__).parent / "listings.db"
JSON_PATH = Path(__file__).parent / "listings_data.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


def safe(val, default=None):
    """Return val if truthy (but allow 0), else default."""
    return val if val is not None else default


class _MedianAgg:
    def __init__(self):
        self.vals = []
    def step(self, v):
        if v is not None:
            self.vals.append(v)
    def finalize(self):
        if not self.vals:
            return None
        s = sorted(self.vals)
        n = len(s)
        mid = n // 2
        return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2


def build_json(conn: sqlite3.Connection) -> dict:
    conn.create_aggregate("MEDIAN", 1, _MedianAgg)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Filter all listing queries to only those active in the latest scrape run
    cur.execute("""
        CREATE TEMP VIEW active_listings AS
        SELECT * FROM listings
        WHERE listing_id IN (
            SELECT listing_id FROM snapshots
            WHERE run_date = (SELECT MAX(run_date) FROM snapshots)
        )
    """)

    today = datetime.now(timezone.utc)
    seven_days_ago = (today - timedelta(days=7)).isoformat()
    thirty_days_ago = (today - timedelta(days=30)).isoformat()

    # ── Summary ───────────────────────────────────────────────────────────────
    cur.execute("""
        SELECT
            COUNT(*)                                          AS total_active,
            ROUND(AVG(CAST(price_numeric AS REAL)) / 1e6, 2) AS avg_price_m,
            ROUND(MEDIAN(days_on_market), 0)                     AS avg_dom
        FROM active_listings
        WHERE price_numeric IS NOT NULL
    """)
    row = dict(cur.fetchone())

    cur.execute("SELECT COUNT(*) AS new_7_days FROM active_listings WHERE days_on_market <= 7")
    new_7 = cur.fetchone()["new_7_days"]

    # Total count (including no-price listings)
    cur.execute("SELECT COUNT(*) AS n FROM active_listings")
    total_all = cur.fetchone()["n"]

    summary = {
        "total_active": total_all,
        "new_7_days":   new_7,
        "avg_price_m":  row["avg_price_m"],
        "avg_dom":      int(row["avg_dom"]) if row["avg_dom"] else None,
    }

    # ── Tenure breakdown ──────────────────────────────────────────────────────
    cur.execute("""
        SELECT
            COALESCE(tenure, 'Not Specified')             AS t,
            COUNT(*)                                       AS count,
            ROUND(AVG(CAST(price_numeric AS REAL))/1e6,2) AS avg_price_m,
            ROUND(MEDIAN(days_on_market), 0)                  AS avg_dom
        FROM active_listings
        GROUP BY COALESCE(tenure, 'Not Specified')
        ORDER BY count DESC
    """)
    tenure = [
        {
            "t":           r["t"],
            "count":       r["count"],
            "avg_price_m": r["avg_price_m"],
            "avg_dom":     int(r["avg_dom"]) if r["avg_dom"] else None,
        }
        for r in cur.fetchall()
    ]

    # ── Price band breakdown ──────────────────────────────────────────────────
    bands_def = [
        ("Under $2M",  0,       2_000_000),
        ("$2M – $3M",  2_000_000, 3_000_000),
        ("$3M – $4M",  3_000_000, 4_000_000),
        ("$4M – $5M",  4_000_000, 5_000_000),
        ("$5M – $7M",  5_000_000, 7_000_000),
        ("$7M+",       7_000_000, 999_999_999),
    ]

    price_bands_detail = []
    for label, lo, hi in bands_def:
        cur.execute("""
            SELECT
                COUNT(*)                            AS total,
                SUM(CASE WHEN tenure = 'Freehold' THEN 1 ELSE 0 END) AS freehold,
                ROUND(MEDIAN(days_on_market), 0)        AS avg_dom
            FROM active_listings
            WHERE price_numeric >= ? AND price_numeric < ?
        """, (lo, hi))
        r = cur.fetchone()
        if r["total"] > 0:
            price_bands_detail.append({
                "band":     label,
                "total":    r["total"],
                "freehold": r["freehold"] or 0,
                "other":    (r["total"] or 0) - (r["freehold"] or 0),
                "avg_dom":  int(r["avg_dom"]) if r["avg_dom"] else None,
            })

    # ── Absorption rate ───────────────────────────────────────────────────────
    # Estimate monthly sales from listings removed in the last 30 days
    # (proxy: listings present in snapshots 30 days ago but not now)
    # Fallback: use 5% of active as a conservative monthly sales estimate
    # when snapshot history is insufficient.

    cur.execute("SELECT COUNT(DISTINCT run_date) AS n FROM snapshots")
    snap_count = cur.fetchone()["n"] if "snapshots" in _table_names(conn) else 0

    est_monthly_sales = None
    if snap_count >= 2:
        cur.execute("""
            SELECT COUNT(*) AS removed
            FROM snapshots
            WHERE run_date = (SELECT MIN(run_date) FROM snapshots)
              AND listing_id NOT IN (
                  SELECT listing_id FROM snapshots
                  WHERE run_date = (SELECT MAX(run_date) FROM snapshots)
              )
        """)
        removed = cur.fetchone()["removed"]
        est_monthly_sales = max(1, removed)
    else:
        est_monthly_sales = max(1, round(total_all * 0.05))

    months_of_supply = round(total_all / est_monthly_sales, 1) if est_monthly_sales else None
    absorption_rate_pct = round((est_monthly_sales / total_all) * 100, 1) if total_all else None

    def market_signal(months):
        if months is None:  return "Unknown"
        if months < 3:      return "Seller's Market"
        if months <= 6:     return "Balanced"
        return "Buyer's Market"

    # Freehold specific
    cur.execute("SELECT COUNT(*) AS n FROM active_listings WHERE tenure = 'Freehold'")
    fh_active = cur.fetchone()["n"]
    fh_est_monthly = max(1, round(fh_active * 0.05))
    fh_months = round(fh_active / fh_est_monthly, 1) if fh_est_monthly else None

    # DOM distribution
    cur.execute("SELECT COUNT(*) FROM active_listings WHERE days_on_market <= 30")
    new_30 = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM active_listings WHERE days_on_market <= 90")
    listed_90 = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM active_listings WHERE days_on_market > 180")
    stale_180 = cur.fetchone()[0]

    absorption = {
        "active_listings":        total_all,
        "est_monthly_sales":      est_monthly_sales,
        "months_of_supply":       months_of_supply,
        "absorption_rate_pct":    absorption_rate_pct,
        "market_signal":          market_signal(months_of_supply),
        "freehold_active":        fh_active,
        "freehold_months_supply": fh_months,
        "freehold_signal":        market_signal(fh_months),
        "new_30_days":            new_30,
        "listed_90_days":         listed_90,
        "stale_180_days":         stale_180,
    }

    # ── All listings (for the Listings tab) ───────────────────────────────────
    cur.execute("""
        SELECT
            listing_id, url, address,
            asking_price, price_numeric, price_method,
            agency, agent,
            days_on_market, tenure,
            bedrooms, bathrooms,
            land_area_sqm, floor_area_sqm
        FROM active_listings
        ORDER BY days_on_market ASC NULLS LAST
    """)
    all_listings = [
        {
            "listing_id":    r["listing_id"],
            "url":           r["url"],
            "address":       r["address"],
            "asking_price":  r["asking_price"],
            "price_numeric": r["price_numeric"],
            "price_method":  r["price_method"],
            "agency":        r["agency"],
            "agent":         r["agent"],
            "days_on_market":r["days_on_market"],
            "tenure":        r["tenure"],
            "bedrooms":      r["bedrooms"],
            "bathrooms":     r["bathrooms"],
            "land_area_sqm": r["land_area_sqm"],
            "floor_area_sqm":r["floor_area_sqm"],
        }
        for r in cur.fetchall()
    ]

    # ── History snapshots (for the History tab and trend charts) ──────────────
    history = []
    if "snapshots" in _table_names(conn):
        cur.execute("SELECT DISTINCT run_date FROM snapshots ORDER BY run_date ASC")
        run_dates = [r[0] for r in cur.fetchall()]

        for rd in run_dates:
            cur.execute("""
                SELECT
                    COUNT(*)                                           AS total_active,
                    SUM(CASE WHEN tenure = 'Freehold'    THEN 1 ELSE 0 END) AS freehold_count,
                    SUM(CASE WHEN tenure = 'Leasehold'   THEN 1 ELSE 0 END) AS leasehold_count,
                    SUM(CASE WHEN tenure = 'Unit Title'  THEN 1 ELSE 0 END) AS unit_title_count,
                    ROUND(AVG(CAST(price_numeric AS REAL))/1e6, 2)    AS avg_price_m,
                    ROUND(MEDIAN(days_on_market), 0)                      AS avg_dom
                FROM snapshots
                WHERE run_date = ?
            """, (rd,))
            snap = dict(cur.fetchone())

            # New listings in last 7 days relative to this snapshot date
            prev_week = (datetime.strptime(rd, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
            cur.execute("""
                SELECT COUNT(*) AS n FROM snapshots
                WHERE run_date = ?
                  AND listing_id NOT IN (
                      SELECT listing_id FROM snapshots WHERE run_date <= ?
                      AND run_date < ?
                  )
            """, (rd, prev_week, rd))
            new_7_snap = cur.fetchone()["n"]

            # Estimate supply for this snapshot
            est = max(1, round((snap["total_active"] or 0) * 0.05))
            mos = round((snap["total_active"] or 0) / est, 1)

            history.append({
                "snapshot_date":    rd,
                "total_active":     snap["total_active"],
                "new_7_days":       new_7_snap,
                "avg_price_m":      snap["avg_price_m"],
                "avg_dom":          int(snap["avg_dom"]) if snap["avg_dom"] else None,
                "freehold_count":   snap["freehold_count"],
                "leasehold_count":  snap["leasehold_count"],
                "unit_title_count": snap["unit_title_count"],
                "est_monthly_sales":est,
                "months_of_supply": mos,
                "market_signal":    market_signal(mos),
            })

    # ── Diff (price reductions + withdrawn) ──────────────────────────────────
    diff = {"price_reductions": None, "withdrawn": None}
    if "snapshots" in _table_names(conn):
        cur.execute("SELECT DISTINCT run_date FROM snapshots ORDER BY run_date DESC LIMIT 2")
        dates = [r[0] for r in cur.fetchall()]
        if len(dates) == 2:
            latest_date, prev_date = dates[0], dates[1]
            cur.execute("""
                SELECT COUNT(*) AS n FROM snapshots
                WHERE run_date = ?
                  AND listing_id NOT IN (
                      SELECT listing_id FROM snapshots WHERE run_date = ?)
            """, (prev_date, latest_date))
            diff["withdrawn"] = cur.fetchone()["n"]
            cur.execute("""
                SELECT COUNT(*) AS n FROM snapshots curr
                JOIN snapshots prev ON curr.listing_id = prev.listing_id
                WHERE curr.run_date = ?
                  AND prev.run_date = ?
                  AND curr.price_numeric IS NOT NULL
                  AND prev.price_numeric IS NOT NULL
                  AND curr.price_numeric < prev.price_numeric
            """, (latest_date, prev_date))
            diff["price_reductions"] = cur.fetchone()["n"]

    # ── Assemble ──────────────────────────────────────────────────────────────
    # ── Queenstown filter ─────────────────────────────────────────────────────
    # Queenstown-area = addresses NOT in the Wanaka district
    # Wanaka district addresses contain ", Wanaka, Central Otago"
    QTN_EXCLUDE = "%, Wanaka, Central Otago%"

    # ── Summary (Queenstown only) ─────────────────────────────────────────────
    cur.execute(f"""
        SELECT
            COUNT(*)                                          AS total_active,
            ROUND(AVG(CAST(price_numeric AS REAL)) / 1e6, 2) AS avg_price_m,
            ROUND(MEDIAN(days_on_market), 0)                     AS avg_dom
        FROM active_listings
        WHERE price_numeric IS NOT NULL
          AND address NOT LIKE '{QTN_EXCLUDE}'
    """)
    row_qtn = dict(cur.fetchone())

    cur.execute(f"""
        SELECT COUNT(*) AS n FROM active_listings
        WHERE days_on_market <= 7
          AND address NOT LIKE '{QTN_EXCLUDE}'
    """)
    new_7_qtn = cur.fetchone()["n"]

    cur.execute(f"SELECT COUNT(*) AS n FROM active_listings WHERE address NOT LIKE '{QTN_EXCLUDE}'")
    total_qtn = cur.fetchone()["n"]

    summary_qtn = {
        "total_active": total_qtn,
        "new_7_days":   new_7_qtn,
        "avg_price_m":  row_qtn["avg_price_m"],
        "avg_dom":      int(row_qtn["avg_dom"]) if row_qtn["avg_dom"] else None,
    }

    # Keep original summary as all-QLDC
    summary_all = summary

    # ── Tenure (Queenstown only) ──────────────────────────────────────────────
    cur.execute(f"""
        SELECT
            COALESCE(tenure, 'Not Specified')             AS t,
            COUNT(*)                                       AS count,
            ROUND(AVG(CAST(price_numeric AS REAL))/1e6,2) AS avg_price_m,
            ROUND(MEDIAN(days_on_market), 0)                  AS avg_dom
        FROM active_listings
        WHERE address NOT LIKE '{QTN_EXCLUDE}'
        GROUP BY COALESCE(tenure, 'Not Specified')
        ORDER BY count DESC
    """)
    tenure_qtn = [
        {"t": r["t"], "count": r["count"],
         "avg_price_m": r["avg_price_m"],
         "avg_dom": int(r["avg_dom"]) if r["avg_dom"] else None}
        for r in cur.fetchall()
    ]

    # ── Price bands (Queenstown only) ─────────────────────────────────────────
    price_bands_qtn = []
    for label, lo, hi in bands_def:
        cur.execute(f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN tenure='Freehold' THEN 1 ELSE 0 END) AS freehold,
                   ROUND(MEDIAN(days_on_market),0) AS avg_dom
            FROM active_listings
            WHERE price_numeric >= ? AND price_numeric < ?
              AND address NOT LIKE '{QTN_EXCLUDE}'
        """, (lo, hi))
        r = cur.fetchone()
        if r["total"] > 0:
            price_bands_qtn.append({
                "band":     label,
                "total":    r["total"],
                "freehold": r["freehold"] or 0,
                "other":    (r["total"] or 0) - (r["freehold"] or 0),
                "avg_dom":  int(r["avg_dom"]) if r["avg_dom"] else None,
            })

    # ── Absorption (Queenstown only) ──────────────────────────────────────────
    est_qtn = max(1, round(total_qtn * 0.05))
    mos_qtn = round(total_qtn / est_qtn, 1) if est_qtn else None

    cur.execute(f"SELECT COUNT(*) AS n FROM active_listings WHERE tenure='Freehold' AND address NOT LIKE '{QTN_EXCLUDE}'")
    fh_qtn = cur.fetchone()["n"]
    fh_est_qtn = max(1, round(fh_qtn * 0.05))
    fh_mos_qtn = round(fh_qtn / fh_est_qtn, 1) if fh_est_qtn else None

    cur.execute(f"SELECT COUNT(*) FROM active_listings WHERE days_on_market <= 30 AND address NOT LIKE '{QTN_EXCLUDE}'")
    new_30_qtn = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM active_listings WHERE days_on_market <= 90 AND address NOT LIKE '{QTN_EXCLUDE}'")
    listed_90_qtn = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM active_listings WHERE days_on_market > 180 AND address NOT LIKE '{QTN_EXCLUDE}'")
    stale_180_qtn = cur.fetchone()[0]

    absorption_qtn = {
        "active_listings":        total_qtn,
        "est_monthly_sales":      est_qtn,
        "months_of_supply":       mos_qtn,
        "absorption_rate_pct":    round((est_qtn / total_qtn)*100, 1) if total_qtn else None,
        "market_signal":          market_signal(mos_qtn),
        "freehold_active":        fh_qtn,
        "freehold_months_supply": fh_mos_qtn,
        "freehold_signal":        market_signal(fh_mos_qtn),
        "new_30_days":            new_30_qtn,
        "listed_90_days":         listed_90_qtn,
        "stale_180_days":         stale_180_qtn,
    }

    absorption_all = absorption


    # Match on agency name containing "Kawarau Heights"
    cur.execute("""
        SELECT
            listing_id, url, address, asking_price, price_numeric, price_method,
            agency, agent, days_on_market, tenure, bedrooms, bathrooms,
            land_area_sqm, floor_area_sqm
        FROM active_listings
        WHERE LOWER(agency) LIKE '%kawarau heights%'
        ORDER BY days_on_market ASC NULLS LAST
    """)
    kh_listings = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT
            COUNT(*) AS count,
            ROUND(AVG(CAST(price_numeric AS REAL))/1e6, 2) AS avg_price_m,
            ROUND(MEDIAN(days_on_market), 0) AS avg_dom
        FROM active_listings
        WHERE LOWER(agency) LIKE '%kawarau heights%'
          AND price_numeric IS NOT NULL
    """)
    kh_s = dict(cur.fetchone())
    kh_summary = {
        "count":       len(kh_listings),
        "avg_price_m": kh_s["avg_price_m"],
        "avg_dom":     int(kh_s["avg_dom"]) if kh_s["avg_dom"] else None,
    }

    # ── Jack's Point listings ─────────────────────────────────────────────────
    # Match addresses containing "Jacks Point", "JACKS POINT", or "Hanley's Farm"
    cur.execute("""
        SELECT
            listing_id, url, address, asking_price, price_numeric, price_method,
            agency, agent, days_on_market, tenure, bedrooms, bathrooms,
            land_area_sqm, floor_area_sqm
        FROM active_listings
        WHERE LOWER(address) LIKE '%jacks point%'
           OR LOWER(address) LIKE '%hanley%farm%'
        ORDER BY days_on_market ASC NULLS LAST
    """)
    jp_listings = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT
            COUNT(*) AS count,
            ROUND(AVG(CAST(price_numeric AS REAL))/1e6, 2) AS avg_price_m,
            ROUND(MEDIAN(days_on_market), 0) AS avg_dom
        FROM active_listings
        WHERE (LOWER(address) LIKE '%jacks point%'
            OR LOWER(address) LIKE '%hanley%farm%')
          AND price_numeric IS NOT NULL
    """)
    jp_s = dict(cur.fetchone())
    jp_summary = {
        "count":       len(jp_listings),
        "avg_price_m": jp_s["avg_price_m"],
        "avg_dom":     int(jp_s["avg_dom"]) if jp_s["avg_dom"] else None,
    }

    return {
        "scrape_date":           today.strftime("%-d %B %Y"),
        "summary":               summary_qtn,
        "summary_qtn":           summary_qtn,
        "summary_all":           summary_all,
        "tenure":                tenure_qtn,
        "tenure_qtn":            tenure_qtn,
        "price_bands_detail":    price_bands_qtn,
        "price_bands_qtn":       price_bands_qtn,
        "price_bands_all":       price_bands_detail,
        "absorption":            absorption_qtn,
        "absorption_qtn":        absorption_qtn,
        "absorption_all":        absorption_all,
        "all_listings":          all_listings,
        "diff":                  diff,
        "history":               history,
        "kh_listings":           kh_listings,
        "kh_summary":            kh_summary,
        "jacks_point_listings":  jp_listings,
        "jacks_point_summary":   jp_summary,
    }


def _table_names(conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {r[0] for r in cur.fetchall()}


def main():
    if not DB_PATH.exists():
        log.error("Database not found: %s", DB_PATH)
        log.error("Run scraper.py first to create the database.")
        return

    log.info("Reading database: %s", DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    try:
        payload = build_json(conn)
    finally:
        conn.close()

    with open(JSON_PATH, "w") as f:
        json.dump(payload, f, indent=2, default=str)

    log.info("Written: %s", JSON_PATH)
    log.info(
        "Summary: %d listings | avg $%sM | avg DOM %s days | %d tenure types",
        payload["summary"]["total_active"],
        payload["summary"]["avg_price_m"],
        payload["summary"]["avg_dom"],
        len(payload["tenure"]),
    )


if __name__ == "__main__":
    main()
