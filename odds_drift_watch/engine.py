"""One tick: fetch, compare, alert."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import httpx

from odds_drift_watch.alerts import build_alert_payload, send_webhook
from odds_drift_watch.odds_api import fetch_event_odds, find_american_price
from odds_drift_watch.store import connect, init_db, list_watches, update_last_quote


def run_tick(
    *,
    db_path: Path,
    api_key: str,
    regions: str = "us",
    markets: str = "h2h",
) -> dict[str, int]:
    """Returns counts: checked, alerts_sent, errors."""
    conn = connect(db_path)
    try:
        init_db(conn)
        watches = list_watches(conn, active_only=True)
        checked = alerts = errors = 0
        # All watches in a group share one price snapshot for this tick.
        snapshots = {}
        failed_groups = set()
        requested = {market.strip() for market in markets.split(",") if market.strip()}
        for w in watches:
            try:
                market_list = ",".join(sorted(requested | {w.market_key}))
                group = (w.sport_key, market_list)
                if group in failed_groups:
                    errors += 1
                    continue
                if group not in snapshots:
                    try:
                        snapshots[group] = fetch_event_odds(
                            api_key, w.sport_key, regions=regions, markets=market_list
                        )
                    except (httpx.HTTPError, ValueError, TypeError):
                        failed_groups.add(group)
                        errors += 1
                        continue
                price = find_american_price(
                    snapshots[group],
                    home_team=w.home_team,
                    away_team=w.away_team,
                    bookmaker_key=w.bookmaker_key,
                    market_key=w.market_key,
                    outcome_name=w.outcome_name,
                )
                checked += 1
                if price is None:
                    errors += 1
                    continue
                now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                prev = w.last_american
                if prev is None:
                    update_last_quote(conn, w.id, price, now)
                    conn.commit()
                    continue
                shock = abs(price - prev)
                if shock >= w.min_american_delta:
                    payload = build_alert_payload(
                        watch_id=w.id,
                        sport_key=w.sport_key,
                        home_team=w.home_team,
                        away_team=w.away_team,
                        bookmaker_key=w.bookmaker_key,
                        outcome_name=w.outcome_name,
                        old_american=prev,
                        new_american=price,
                        line_shock_index=shock,
                    )
                    send_webhook(w.webhook_url, payload)
                    alerts += 1
                update_last_quote(conn, w.id, price, now)
                conn.commit()
            except (httpx.HTTPError, sqlite3.Error, ValueError, TypeError, AttributeError):
                errors += 1
                conn.rollback()
        return {"checked": checked, "alerts_sent": alerts, "errors": errors}
    finally:
        conn.close()
