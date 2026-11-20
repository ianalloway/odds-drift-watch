from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from odds_drift_watch.engine import run_tick
from odds_drift_watch.store import connect, init_db, insert_watch


def sample_events(home="Lakers", away="Celtics", price=-110.0):
    return [
        {
            "home_team": home,
            "away_team": away,
            "bookmakers": [
                {
                    "key": "draftkings",
                    "markets": [
                        {
                            "key": "h2h",
                            "outcomes": [
                                {"name": home, "price": price},
                                {"name": away, "price": 100.0},
                            ],
                        }
                    ],
                }
            ],
        }
    ]


def test_tick_baseline_then_alert(tmp_path: Path):
    db = tmp_path / "t.db"
    conn = connect(db)
    init_db(conn)
    insert_watch(
        conn,
        sport_key="basketball_nba",
        home_team="Lakers",
        away_team="Celtics",
        bookmaker_key="draftkings",
        market_key="h2h",
        outcome_name="Lakers",
        min_american_delta=5.0,
        webhook_url="https://example.com/hook",
    )
    conn.commit()
    conn.close()

    with patch("odds_drift_watch.engine.fetch_event_odds") as m_fetch, patch(
        "odds_drift_watch.engine.send_webhook"
    ) as m_hook:
        m_fetch.return_value = sample_events(price=-110)
        s1 = run_tick(db_path=db, api_key="fake")
        assert s1["alerts_sent"] == 0
        m_fetch.return_value = sample_events(price=-120)
        s2 = run_tick(db_path=db, api_key="fake")
        assert s2["alerts_sent"] == 1
        m_hook.assert_called_once()

def add_watches(db, count, baseline=None):
    with connect(db) as conn:
        init_db(conn)
        for _ in range(count):
            insert_watch(conn, sport_key="basketball_nba", home_team="Lakers",
                         away_team="Celtics", bookmaker_key="draftkings", market_key="h2h",
                         outcome_name="Lakers", min_american_delta=5.0,
                         webhook_url="https://example.com/hook")
        conn.execute("UPDATE watches SET last_american = ?", (baseline,))


@pytest.mark.parametrize("fails", [False, True])
def test_shared_feed_is_fetched_once_per_tick(tmp_path, fails):
    db = tmp_path / "w.db"
    add_watches(db, 3)
    with patch("odds_drift_watch.engine.fetch_event_odds") as fetch:
        if fails:
            fetch.side_effect = httpx.ConnectError("offline")
        else:
            fetch.return_value = sample_events()
        stats = run_tick(db_path=db, api_key="fake")
        fetch.assert_called_once_with("fake", "basketball_nba", regions="us", markets="h2h")
    assert stats == {"checked": 0 if fails else 3, "alerts_sent": 0, "errors": 3 if fails else 0}


def test_rejected_delivery_preserves_baseline_for_retry(tmp_path):
    db = tmp_path / "w.db"
    add_watches(db, 1, baseline=-110)
    with patch("odds_drift_watch.engine.fetch_event_odds", return_value=sample_events(price=-120)), patch(
        "odds_drift_watch.alerts.httpx.Client"
    ) as client:
        post = client.return_value.__enter__.return_value.post
        post.return_value = httpx.Response(503, request=httpx.Request("POST", "https://example.com"))
        assert run_tick(db_path=db, api_key="fake") == {"checked": 1, "alerts_sent": 0, "errors": 1}
        with connect(db) as conn:
            assert conn.execute("SELECT last_american FROM watches").fetchone()[0] == -110
        post.return_value = httpx.Response(204, request=httpx.Request("POST", "https://example.com"))
        assert run_tick(db_path=db, api_key="fake") == {"checked": 1, "alerts_sent": 1, "errors": 0}
