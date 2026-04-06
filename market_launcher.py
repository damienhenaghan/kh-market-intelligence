#!/usr/bin/env python3
"""
market_launcher.py — Kawarau Heights Market Intelligence Dashboard
===================================================================
Double-click this file to launch the dashboard.

What it does:
  1. Reads listings.db and regenerates listings_data.json
  2. Starts a local web server
  3. Opens the dashboard in your browser

Requires scraper.py to have been run at least once.
Keep this window open while using the dashboard.
"""

import sys
import os
import json
import threading
import time
import webbrowser
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from http.server import HTTPServer, SimpleHTTPRequestHandler

BASE_DIR  = Path(__file__).parent.resolve()
DB_PATH   = BASE_DIR / "listings.db"
JSON_PATH = BASE_DIR / "listings_data.json"
HTML_FILE = BASE_DIR / "Index.html"
PORT      = 8743


# ── Import or inline the export logic ────────────────────────────────────────

def _table_names(conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {r[0] for r in cur.fetchall()}


def build_json(conn):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    today           = datetime.now(timezone.utc)
    seven_days_ago  = (today - timedelta(days=7)).isoformat()

    cur.execute("""
        SELECT
            COUNT(*)                                          AS total_active,
            ROUND(AVG(CAST(price_numeric AS REAL)) / 1e6, 2) AS avg_price_m,
            ROUND(AVG(days_on_market), 0)                     AS avg_dom
        FROM listings
        WHERE price_numeric IS NOT NULL
    """)
    row = dict(cur.fetchone())

    cur.execute("SELECT COUNT(*) AS n FROM listings")
    total_all = cur.fetchone()["n"]

    cur.execute("SELECT COUNT(*) AS n FROM listings WHERE days_on_market <= 7")
    new_7 = cur.fetchone()["n"]

    summary = {
        "total_active": total_all,
        "new_7_days":   new_7,
        "avg_price_m":  row["avg_price_m"],
        "avg_dom":      int(row["avg_dom"]) if row["avg_dom"] else None,
    }

    cur.execute("""
        SELECT
            COALESCE(tenure, 'Not Specified')             AS t,
            COUNT(*)                                       AS count,
            ROUND(AVG(CAST(price_numeric AS REAL))/1e6,2) AS avg_price_m,
            ROUND(AVG(days_on_market), 0)                  AS avg_dom
        FROM listings
        GROUP BY COALESCE(tenure, 'Not Specified')
        ORDER BY count DESC
    """)
    tenure = [
        {"t": r["t"], "count": r["count"],
         "avg_price_m": r["avg_price_m"],
         "avg_dom": int(r["avg_dom"]) if r["avg_dom"] else None}
        for r in cur.fetchall()
    ]

    bands_def = [
        ("Under $2M",  0,         2_000_000),
        ("$2M – $3M",  2_000_000, 3_000_000),
        ("$3M – $4M",  3_000_000, 4_000_000),
        ("$4M – $5M",  4_000_000, 5_000_000),
        ("$5M – $7M",  5_000_000, 7_000_000),
        ("$7M+",       7_000_000, 999_999_999),
    ]
    price_bands_detail = []
    for label, lo, hi in bands_def:
        cur.execute("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN tenure='Freehold' THEN 1 ELSE 0 END) AS freehold,
                   ROUND(AVG(days_on_market),0) AS avg_dom
            FROM listings WHERE price_numeric >= ? AND price_numeric < ?
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

    # Absorption
    snap_count = 0
    if "snapshots" in _table_names(conn):
        cur.execute("SELECT COUNT(DISTINCT run_date) AS n FROM snapshots")
        snap_count = cur.fetchone()["n"]

    if snap_count >= 2:
        cur.execute("""
            SELECT COUNT(*) AS removed FROM snapshots
            WHERE run_date=(SELECT MIN(run_date) FROM snapshots)
              AND listing_id NOT IN (
                  SELECT listing_id FROM snapshots
                  WHERE run_date=(SELECT MAX(run_date) FROM snapshots))
        """)
        est_monthly_sales = max(1, cur.fetchone()["removed"])
    else:
        est_monthly_sales = max(1, round(total_all * 0.05))

    months_of_supply    = round(total_all / est_monthly_sales, 1) if est_monthly_sales else None
    absorption_rate_pct = round((est_monthly_sales / total_all)*100, 1) if total_all else None

    def market_signal(m):
        if m is None:  return "Unknown"
        if m < 3:      return "Seller's Market"
        if m <= 6:     return "Balanced"
        return "Buyer's Market"

    cur.execute("SELECT COUNT(*) AS n FROM listings WHERE tenure='Freehold'")
    fh_active = cur.fetchone()["n"]
    fh_est    = max(1, round(fh_active * 0.05))
    fh_months = round(fh_active / fh_est, 1) if fh_est else None

    cur.execute("SELECT COUNT(*) FROM listings WHERE days_on_market <= 30")
    new_30 = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM listings WHERE days_on_market <= 90")
    listed_90 = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM listings WHERE days_on_market > 180")
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

    cur.execute("""
        SELECT listing_id, url, address, asking_price, price_numeric, price_method,
               agency, agent, days_on_market, tenure, bedrooms, bathrooms,
               land_area_sqm, floor_area_sqm
        FROM listings ORDER BY days_on_market ASC NULLS LAST
    """)
    all_listings = [dict(r) for r in cur.fetchall()]

    history = []
    if "snapshots" in _table_names(conn):
        cur.execute("SELECT DISTINCT run_date FROM snapshots ORDER BY run_date ASC")
        run_dates = [r[0] for r in cur.fetchall()]
        for rd in run_dates:
            cur.execute("""
                SELECT COUNT(*) AS total_active,
                       SUM(CASE WHEN tenure='Freehold'   THEN 1 ELSE 0 END) AS freehold_count,
                       SUM(CASE WHEN tenure='Leasehold'  THEN 1 ELSE 0 END) AS leasehold_count,
                       SUM(CASE WHEN tenure='Unit Title' THEN 1 ELSE 0 END) AS unit_title_count,
                       ROUND(AVG(CAST(price_numeric AS REAL))/1e6,2) AS avg_price_m,
                       ROUND(AVG(days_on_market),0) AS avg_dom
                FROM snapshots WHERE run_date=?
            """, (rd,))
            snap = dict(cur.fetchone())
            est  = max(1, round((snap["total_active"] or 0) * 0.05))
            mos  = round((snap["total_active"] or 0) / est, 1)
            history.append({
                "snapshot_date":    rd,
                "total_active":     snap["total_active"],
                "new_7_days":       None,
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

            # Withdrawn: in previous snapshot but not in latest
            cur.execute("""
                SELECT COUNT(*) AS n FROM snapshots
                WHERE run_date = ?
                  AND listing_id NOT IN (
                      SELECT listing_id FROM snapshots WHERE run_date = ?)
            """, (prev_date, latest_date))
            diff["withdrawn"] = cur.fetchone()["n"]

            # Price reductions: same listing, lower price this week vs last
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

    # ── Queenstown split ──────────────────────────────────────────────────────
    QX = "%, Wanaka, Central Otago%"
    cur.execute(f"""
        SELECT COUNT(*) AS n, ROUND(AVG(CAST(price_numeric AS REAL))/1e6,2) AS avg_price_m,
               ROUND(AVG(days_on_market),0) AS avg_dom
        FROM listings WHERE price_numeric IS NOT NULL AND address NOT LIKE '{QX}'
    """)
    rq = dict(cur.fetchone())
    cur.execute(f"SELECT COUNT(*) AS n FROM listings WHERE address NOT LIKE '{QX}'")
    total_qtn = cur.fetchone()["n"]
    cur.execute(f"SELECT COUNT(*) AS n FROM listings WHERE days_on_market <= 7 AND address NOT LIKE '{QX}'")
    new_7_qtn = cur.fetchone()["n"]
    summary_qtn = {"total_active": total_qtn, "new_7_days": new_7_qtn,
                   "avg_price_m": rq["avg_price_m"],
                   "avg_dom": int(rq["avg_dom"]) if rq["avg_dom"] else None}
    summary_all = summary

    cur.execute(f"""
        SELECT COALESCE(tenure,'Not Specified') AS t, COUNT(*) AS count,
               ROUND(AVG(CAST(price_numeric AS REAL))/1e6,2) AS avg_price_m,
               ROUND(AVG(days_on_market),0) AS avg_dom
        FROM listings WHERE address NOT LIKE '{QX}'
        GROUP BY COALESCE(tenure,'Not Specified') ORDER BY count DESC
    """)
    tenure_qtn = [{"t":r["t"],"count":r["count"],"avg_price_m":r["avg_price_m"],
                   "avg_dom":int(r["avg_dom"]) if r["avg_dom"] else None} for r in cur.fetchall()]

    bands_def2 = [("Under $2M",0,2e6),("$2M \u2013 $3M",2e6,3e6),("$3M \u2013 $4M",3e6,4e6),
                  ("$4M \u2013 $5M",4e6,5e6),("$5M \u2013 $7M",5e6,7e6),("$7M+",7e6,999999999)]
    price_bands_qtn = []
    for label, lo, hi in bands_def2:
        cur.execute(f"""SELECT COUNT(*) AS total, SUM(CASE WHEN tenure='Freehold' THEN 1 ELSE 0 END) AS freehold,
               ROUND(AVG(days_on_market),0) AS avg_dom FROM listings
               WHERE price_numeric>=? AND price_numeric<? AND address NOT LIKE '{QX}'""", (lo, hi))
        r = cur.fetchone()
        if r["total"] > 0:
            price_bands_qtn.append({"band":label,"total":r["total"],"freehold":r["freehold"] or 0,
                "other":(r["total"] or 0)-(r["freehold"] or 0),"avg_dom":int(r["avg_dom"]) if r["avg_dom"] else None})

    est_qtn = max(1, round(total_qtn * 0.05))
    mos_qtn = round(total_qtn / est_qtn, 1)
    cur.execute(f"SELECT COUNT(*) AS n FROM listings WHERE tenure='Freehold' AND address NOT LIKE '{QX}'")
    fh_qtn = cur.fetchone()["n"]
    fh_est_qtn = max(1, round(fh_qtn * 0.05))
    fh_mos_qtn = round(fh_qtn / fh_est_qtn, 1)
    cur.execute(f"SELECT COUNT(*) FROM listings WHERE days_on_market<=30 AND address NOT LIKE '{QX}'")
    new30q = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM listings WHERE days_on_market<=90 AND address NOT LIKE '{QX}'")
    l90q = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM listings WHERE days_on_market>180 AND address NOT LIKE '{QX}'")
    s180q = cur.fetchone()[0]
    absorption_qtn = {"active_listings":total_qtn,"est_monthly_sales":est_qtn,"months_of_supply":mos_qtn,
        "absorption_rate_pct":round((est_qtn/total_qtn)*100,1) if total_qtn else None,
        "market_signal":market_signal(mos_qtn),"freehold_active":fh_qtn,
        "freehold_months_supply":fh_mos_qtn,"freehold_signal":market_signal(fh_mos_qtn),
        "new_30_days":new30q,"listed_90_days":l90q,"stale_180_days":s180q}
    absorption_all = absorption

    # KH listings
    cur.execute("""
        SELECT listing_id, url, address, asking_price, price_numeric, price_method,
               agency, agent, days_on_market, tenure, bedrooms, bathrooms,
               land_area_sqm, floor_area_sqm
        FROM listings
        WHERE LOWER(agency) LIKE '%kawarau heights%'
        ORDER BY days_on_market ASC NULLS LAST
    """)
    kh_listings = [dict(r) for r in cur.fetchall()]
    cur.execute("""
        SELECT COUNT(*) AS count,
               ROUND(AVG(CAST(price_numeric AS REAL))/1e6,2) AS avg_price_m,
               ROUND(AVG(days_on_market),0) AS avg_dom
        FROM listings
        WHERE LOWER(agency) LIKE '%kawarau heights%' AND price_numeric IS NOT NULL
    """)
    kh_s = dict(cur.fetchone())
    kh_summary = {
        "count":       len(kh_listings),
        "avg_price_m": kh_s["avg_price_m"],
        "avg_dom":     int(kh_s["avg_dom"]) if kh_s["avg_dom"] else None,
    }

    # Jack's Point listings
    cur.execute("""
        SELECT listing_id, url, address, asking_price, price_numeric, price_method,
               agency, agent, days_on_market, tenure, bedrooms, bathrooms,
               land_area_sqm, floor_area_sqm
        FROM listings
        WHERE LOWER(address) LIKE '%jacks point%'
           OR LOWER(address) LIKE '%hanley%farm%'
        ORDER BY days_on_market ASC NULLS LAST
    """)
    jp_listings = [dict(r) for r in cur.fetchall()]
    cur.execute("""
        SELECT COUNT(*) AS count,
               ROUND(AVG(CAST(price_numeric AS REAL))/1e6,2) AS avg_price_m,
               ROUND(AVG(days_on_market),0) AS avg_dom
        FROM listings
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
        "kh_listings":           kh_listings,
        "kh_summary":            kh_summary,
        "jacks_point_listings":  jp_listings,
        "jacks_point_summary":   jp_summary,
    }


# ── HTTP handler — serves files from BASE_DIR ────────────────────────────────

class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(BASE_DIR), **kwargs)

    def log_message(self, format, *args):
        pass  # Keep terminal clean


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print()
    print("  Kawarau Heights — Market Intelligence Dashboard")
    print("  " + "─" * 48)

    if not HTML_FILE.exists():
        print(f"\n  ERROR: Index.html not found in {BASE_DIR}")
        print("  Make sure Index.html is in the same folder as this launcher.\n")
        input("Press Enter to exit...")
        sys.exit(1)

    if not DB_PATH.exists():
        print(f"\n  ERROR: listings.db not found.")
        print("  Run scraper.py first to build the database.\n")
        input("Press Enter to exit...")
        sys.exit(1)

    # Generate JSON from database
    print("  Generating listings_data.json from database...")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        payload = build_json(conn)
        conn.close()
        with open(JSON_PATH, "w") as f:
            json.dump(payload, f, indent=2, default=str)
        print(f"  Done. {payload['summary']['total_active']} listings exported.")
    except Exception as e:
        print(f"\n  ERROR generating JSON: {e}")
        input("Press Enter to exit...")
        sys.exit(1)

    # Start server
    server = HTTPServer(("localhost", PORT), Handler)

    print(f"\n  Running at http://localhost:{PORT}")
    print("  Opening dashboard in browser...")
    print("\n  Keep this window open while using the dashboard.")
    print("  Press Ctrl+C to stop.\n")

    def open_browser():
        time.sleep(0.8)
        webbrowser.open(f"http://localhost:{PORT}/Index.html")

    threading.Thread(target=open_browser, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Dashboard closed.\n")


if __name__ == "__main__":
    main()
