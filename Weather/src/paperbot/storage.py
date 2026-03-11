from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


class WeatherBotStorage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS bot_runs (
                    run_id TEXT PRIMARY KEY,
                    generated_at TEXT NOT NULL,
                    raw_count INTEGER NOT NULL,
                    count_selected INTEGER NOT NULL,
                    filters_json TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS opportunities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    rank INTEGER NOT NULL,
                    city_key TEXT NOT NULL,
                    city_name TEXT NOT NULL,
                    day_label TEXT NOT NULL,
                    date_str TEXT NOT NULL,
                    event_slug TEXT NOT NULL,
                    event_title TEXT NOT NULL,
                    bucket TEXT NOT NULL,
                    side TEXT NOT NULL,
                    edge REAL NOT NULL,
                    ev_percent REAL NOT NULL,
                    price_cents REAL NOT NULL,
                    model_prob REAL NOT NULL,
                    market_prob REAL NOT NULL,
                    ensemble_prediction REAL NOT NULL,
                    weighted_score REAL NOT NULL,
                    consensus_score REAL NOT NULL,
                    spread REAL NOT NULL,
                    sigma REAL NOT NULL,
                    token_id TEXT,
                    market_slug TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    best_ask REAL,
                    last_trade_price REAL,
                    order_min_size REAL,
                    model_predictions_json TEXT NOT NULL,
                    agreement_models INTEGER NOT NULL DEFAULT 0,
                    total_models INTEGER NOT NULL DEFAULT 0,
                    agreement_pct REAL NOT NULL DEFAULT 0,
                    confidence_tier TEXT NOT NULL DEFAULT 'risky',
                    policy_allowed INTEGER NOT NULL DEFAULT 0,
                    policy_reason TEXT,
                    polymarket_url TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES bot_runs(run_id)
                );

                CREATE INDEX IF NOT EXISTS idx_opportunities_run_id ON opportunities(run_id);
                CREATE INDEX IF NOT EXISTS idx_opportunities_event_slug ON opportunities(event_slug);
                CREATE INDEX IF NOT EXISTS idx_opportunities_date_str ON opportunities(date_str);

                CREATE TABLE IF NOT EXISTS scan_predictions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    selected_rank INTEGER,
                    is_selected INTEGER NOT NULL DEFAULT 0,
                    city_key TEXT NOT NULL,
                    city_name TEXT NOT NULL,
                    day_label TEXT NOT NULL,
                    date_str TEXT NOT NULL,
                    event_slug TEXT NOT NULL,
                    event_title TEXT NOT NULL,
                    bucket TEXT NOT NULL,
                    side TEXT NOT NULL,
                    edge REAL NOT NULL,
                    ev_percent REAL NOT NULL,
                    price_cents REAL NOT NULL,
                    model_prob REAL NOT NULL,
                    market_prob REAL NOT NULL,
                    ensemble_prediction REAL NOT NULL,
                    weighted_score REAL NOT NULL,
                    consensus_score REAL NOT NULL,
                    spread REAL NOT NULL,
                    sigma REAL NOT NULL,
                    agreement_models INTEGER NOT NULL DEFAULT 0,
                    total_models INTEGER NOT NULL DEFAULT 0,
                    agreement_pct REAL NOT NULL DEFAULT 0,
                    confidence_tier TEXT NOT NULL DEFAULT 'risky',
                    policy_allowed INTEGER NOT NULL DEFAULT 0,
                    policy_reason TEXT,
                    token_id TEXT,
                    market_slug TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    model_predictions_json TEXT NOT NULL,
                    price_source TEXT,
                    reference_price_cents REAL,
                    best_bid_cents REAL,
                    settled_price_cents REAL,
                    pnl_usd REAL,
                    roi_percent REAL,
                    resolution_source TEXT,
                    resolved_by TEXT,
                    resolved_at TEXT,
                    polymarket_url TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_scan_predictions_run_id ON scan_predictions(run_id);
                CREATE INDEX IF NOT EXISTS idx_scan_predictions_market ON scan_predictions(market_slug, side);

                CREATE TABLE IF NOT EXISTS order_plans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    rank INTEGER NOT NULL,
                    event_slug TEXT NOT NULL,
                    market_slug TEXT NOT NULL,
                    side TEXT NOT NULL,
                    token_id TEXT,
                    limit_price_cents REAL NOT NULL,
                    model_prob REAL NOT NULL,
                    market_prob REAL NOT NULL,
                    edge REAL NOT NULL,
                    ev_percent REAL NOT NULL,
                    confidence_tier TEXT NOT NULL,
                    bankroll_usd REAL NOT NULL,
                    stake_fraction REAL NOT NULL,
                    stake_usd REAL NOT NULL,
                    share_size REAL NOT NULL,
                    polymarket_url TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES bot_runs(run_id)
                );

                CREATE INDEX IF NOT EXISTS idx_order_plans_run_id ON order_plans(run_id);

                CREATE TABLE IF NOT EXISTS executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    rank INTEGER NOT NULL,
                    mode TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    accepted INTEGER NOT NULL DEFAULT 0,
                    market_slug TEXT NOT NULL,
                    event_slug TEXT,
                    side TEXT NOT NULL,
                    token_id TEXT,
                    client_order_id TEXT,
                    exchange_order_id TEXT,
                    order_status TEXT,
                    filled_shares REAL,
                    avg_fill_price_cents REAL,
                    price_cents REAL NOT NULL,
                    share_size REAL NOT NULL,
                    error TEXT,
                    response_json TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES bot_runs(run_id)
                );

                CREATE INDEX IF NOT EXISTS idx_executions_run_id ON executions(run_id);
                CREATE INDEX IF NOT EXISTS idx_executions_success ON executions(success);

                CREATE TABLE IF NOT EXISTS live_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_order_id TEXT UNIQUE NOT NULL,
                    exchange_order_id TEXT,
                    run_id TEXT,
                    rank INTEGER,
                    event_slug TEXT,
                    market_slug TEXT NOT NULL,
                    city_key TEXT,
                    date_str TEXT,
                    side TEXT NOT NULL,
                    token_id TEXT,
                    nonce INTEGER,
                    submission_fingerprint TEXT,
                    polymarket_url TEXT,
                    requested_price_cents REAL NOT NULL,
                    requested_shares REAL NOT NULL,
                    filled_shares REAL NOT NULL DEFAULT 0,
                    avg_fill_price_cents REAL,
                    status TEXT NOT NULL DEFAULT 'unknown',
                    accepted INTEGER NOT NULL DEFAULT 0,
                    order_min_size REAL,
                    tick_size_cents REAL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    raw_response_json TEXT NOT NULL DEFAULT '{}',
                    error TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_live_orders_status ON live_orders(status);
                CREATE INDEX IF NOT EXISTS idx_live_orders_market ON live_orders(market_slug, side);

                CREATE TABLE IF NOT EXISTS live_fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fill_id TEXT UNIQUE,
                    client_order_id TEXT NOT NULL,
                    exchange_order_id TEXT,
                    run_id TEXT,
                    rank INTEGER,
                    event_slug TEXT,
                    market_slug TEXT NOT NULL,
                    side TEXT NOT NULL,
                    token_id TEXT,
                    share_size REAL NOT NULL,
                    fill_price_cents REAL NOT NULL,
                    notional_usd REAL NOT NULL,
                    filled_at TEXT,
                    raw_fill_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_live_fills_market ON live_fills(market_slug, side);
                CREATE INDEX IF NOT EXISTS idx_live_fills_client_order ON live_fills(client_order_id);

                CREATE TABLE IF NOT EXISTS positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    rank INTEGER NOT NULL,
                    mode TEXT NOT NULL,
                    client_order_id TEXT,
                    exchange_order_id TEXT,
                    event_slug TEXT NOT NULL,
                    market_slug TEXT NOT NULL,
                    city_key TEXT NOT NULL,
                    date_str TEXT NOT NULL,
                    side TEXT NOT NULL,
                    token_id TEXT,
                    entry_price_cents REAL NOT NULL,
                    share_size REAL NOT NULL,
                    stake_usd REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    settled_price_cents REAL,
                    payout_usd REAL,
                    pnl_usd REAL,
                    roi_percent REAL,
                    resolution_source TEXT,
                    resolved_by TEXT,
                    opened_at TEXT NOT NULL,
                    resolved_at TEXT,
                    notes TEXT,
                    polymarket_url TEXT NOT NULL,
                    UNIQUE(run_id, rank)
                );

                CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
                CREATE INDEX IF NOT EXISTS idx_positions_market_slug ON positions(market_slug);
                CREATE INDEX IF NOT EXISTS idx_positions_event_slug ON positions(event_slug);

                CREATE TABLE IF NOT EXISTS live_account_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    saldo_usd REAL,
                    portfolio_usd REAL,
                    total_net_worth_usd REAL,
                    total_open_pnl_usd REAL,
                    open_positions_count INTEGER NOT NULL DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_live_account_snapshots_captured_at
                ON live_account_snapshots(captured_at);

                CREATE TABLE IF NOT EXISTS market_history_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'gamma_clob',
                    city_key TEXT,
                    date_str TEXT,
                    event_slug TEXT NOT NULL,
                    event_title TEXT,
                    market_slug TEXT NOT NULL,
                    market_id TEXT,
                    bucket TEXT,
                    token_id_yes TEXT,
                    token_id_no TEXT,
                    yes_price_cents REAL,
                    no_price_cents REAL,
                    yes_best_ask_cents REAL,
                    no_best_ask_cents REAL,
                    yes_best_bid_cents REAL,
                    no_best_bid_cents REAL,
                    last_trade_price REAL,
                    order_min_size REAL,
                    raw_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_market_history_snapshots_market
                ON market_history_snapshots(market_slug, captured_at);
                CREATE INDEX IF NOT EXISTS idx_market_history_snapshots_event
                ON market_history_snapshots(event_slug, captured_at);

                CREATE TABLE IF NOT EXISTS forecast_source_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    captured_at TEXT NOT NULL,
                    city_key TEXT NOT NULL,
                    city_name TEXT,
                    day_label TEXT,
                    date_str TEXT NOT NULL,
                    event_slug TEXT NOT NULL,
                    market_slug TEXT NOT NULL,
                    market_id TEXT,
                    bucket TEXT NOT NULL,
                    side TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    forecast_temp_f REAL NOT NULL,
                    effective_weight REAL,
                    agreement_models INTEGER,
                    total_models INTEGER,
                    agreement_pct REAL,
                    aligns_with_trade_side INTEGER,
                    source_in_bucket INTEGER,
                    source_delta_f REAL,
                    raw_context_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_forecast_source_snapshots_run
                ON forecast_source_snapshots(run_id, source_name);
                CREATE INDEX IF NOT EXISTS idx_forecast_source_snapshots_city_date
                ON forecast_source_snapshots(city_key, date_str, source_name);
                CREATE INDEX IF NOT EXISTS idx_forecast_source_snapshots_market
                ON forecast_source_snapshots(market_slug, source_name, captured_at);

                CREATE TABLE IF NOT EXISTS station_observation_daily_highs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    city_key TEXT NOT NULL,
                    city_name TEXT,
                    station_id TEXT,
                    local_date TEXT NOT NULL,
                    observed_high_f REAL NOT NULL,
                    source TEXT NOT NULL DEFAULT 'nws_station_observation',
                    raw_context_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_station_observation_daily_highs_city_date
                ON station_observation_daily_highs(city_key, local_date);
                CREATE INDEX IF NOT EXISTS idx_station_observation_daily_highs_station
                ON station_observation_daily_highs(station_id, local_date);
                """
            )
            self._ensure_opportunities_columns(conn)
            self._ensure_scan_predictions_columns(conn)
            self._ensure_executions_columns(conn)
            self._ensure_positions_columns(conn)
            self._ensure_live_orders_columns(conn)
            conn.execute("DELETE FROM positions WHERE mode != 'live'")
            self._backfill_positions(conn)

    def _ensure_opportunities_columns(self, conn: sqlite3.Connection) -> None:
        existing = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(opportunities)").fetchall()
        }
        required = {
            "price_source": "TEXT",
            "reference_price_cents": "REAL",
            "best_bid_cents": "REAL",
            "coverage_ok": "INTEGER NOT NULL DEFAULT 0",
            "coverage_score": "REAL NOT NULL DEFAULT 0",
            "coverage_issue_type": "TEXT",
            "degraded_reason": "TEXT",
            "agreement_models": "INTEGER NOT NULL DEFAULT 0",
            "total_models": "INTEGER NOT NULL DEFAULT 0",
            "agreement_pct": "REAL NOT NULL DEFAULT 0",
            "agreement_summary": "TEXT",
            "confidence_tier": "TEXT NOT NULL DEFAULT 'risky'",
            "signal_tier": "TEXT NOT NULL DEFAULT 'C'",
            "signal_decision": "TEXT",
            "mean_agreeing_model_edge": "REAL NOT NULL DEFAULT 0",
            "min_agreeing_model_edge": "REAL NOT NULL DEFAULT 0",
            "agreeing_model_count": "INTEGER NOT NULL DEFAULT 0",
            "executable_quality_score": "REAL NOT NULL DEFAULT 0",
            "data_quality_score": "REAL NOT NULL DEFAULT 0",
            "valid_model_count": "INTEGER NOT NULL DEFAULT 0",
            "required_model_count": "INTEGER NOT NULL DEFAULT 0",
            "provider_failures_json": "TEXT NOT NULL DEFAULT '[]'",
            "provider_failure_details_json": "TEXT NOT NULL DEFAULT '{}'",
            "effective_weights_json": "TEXT NOT NULL DEFAULT '{}'",
            "adversarial_score": "REAL NOT NULL DEFAULT 0",
            "execution_priority_score": "REAL NOT NULL DEFAULT 0",
            "policy_allowed": "INTEGER NOT NULL DEFAULT 0",
            "policy_reason": "TEXT",
        }
        for column_name, column_type in required.items():
            if column_name not in existing:
                conn.execute(f"ALTER TABLE opportunities ADD COLUMN {column_name} {column_type}")

    def _ensure_scan_predictions_columns(self, conn: sqlite3.Connection) -> None:
        existing = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(scan_predictions)").fetchall()
        }
        required = {
            "policy_allowed": "INTEGER NOT NULL DEFAULT 0",
            "policy_reason": "TEXT",
            "coverage_ok": "INTEGER NOT NULL DEFAULT 0",
            "coverage_score": "REAL NOT NULL DEFAULT 0",
            "coverage_issue_type": "TEXT",
            "degraded_reason": "TEXT",
            "best_ask": "REAL",
            "last_trade_price": "REAL",
            "order_min_size": "REAL",
            "agreement_summary": "TEXT",
            "signal_tier": "TEXT NOT NULL DEFAULT 'C'",
            "signal_decision": "TEXT",
            "mean_agreeing_model_edge": "REAL NOT NULL DEFAULT 0",
            "min_agreeing_model_edge": "REAL NOT NULL DEFAULT 0",
            "agreeing_model_count": "INTEGER NOT NULL DEFAULT 0",
            "executable_quality_score": "REAL NOT NULL DEFAULT 0",
            "data_quality_score": "REAL NOT NULL DEFAULT 0",
            "valid_model_count": "INTEGER NOT NULL DEFAULT 0",
            "required_model_count": "INTEGER NOT NULL DEFAULT 0",
            "provider_failures_json": "TEXT NOT NULL DEFAULT '[]'",
            "provider_failure_details_json": "TEXT NOT NULL DEFAULT '{}'",
            "effective_weights_json": "TEXT NOT NULL DEFAULT '{}'",
            "adversarial_score": "REAL NOT NULL DEFAULT 0",
            "execution_priority_score": "REAL NOT NULL DEFAULT 0",
            "settled_price_cents": "REAL",
            "pnl_usd": "REAL",
            "roi_percent": "REAL",
            "resolution_source": "TEXT",
            "resolved_by": "TEXT",
            "resolved_at": "TEXT",
        }
        for column_name, column_type in required.items():
            if column_name not in existing:
                conn.execute(f"ALTER TABLE scan_predictions ADD COLUMN {column_name} {column_type}")

    def _ensure_executions_columns(self, conn: sqlite3.Connection) -> None:
        existing = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(executions)").fetchall()
        }
        required = {
            "accepted": "INTEGER NOT NULL DEFAULT 0",
            "event_slug": "TEXT",
            "token_id": "TEXT",
            "client_order_id": "TEXT",
            "nonce": "INTEGER",
            "submission_fingerprint": "TEXT",
            "exchange_order_id": "TEXT",
            "order_status": "TEXT",
            "filled_shares": "REAL",
            "avg_fill_price_cents": "REAL",
        }
        for column_name, column_type in required.items():
            if column_name not in existing:
                conn.execute(f"ALTER TABLE executions ADD COLUMN {column_name} {column_type}")

    def _ensure_positions_columns(self, conn: sqlite3.Connection) -> None:
        existing = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(positions)").fetchall()
        }
        required = {
            "client_order_id": "TEXT",
            "exchange_order_id": "TEXT",
        }
        for column_name, column_type in required.items():
            if column_name not in existing:
                conn.execute(f"ALTER TABLE positions ADD COLUMN {column_name} {column_type}")

    def _ensure_live_orders_columns(self, conn: sqlite3.Connection) -> None:
        existing = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(live_orders)").fetchall()
        }
        required = {
            "city_key": "TEXT",
            "date_str": "TEXT",
            "polymarket_url": "TEXT",
            "nonce": "INTEGER",
            "submission_fingerprint": "TEXT",
        }
        for column_name, column_type in required.items():
            if column_name not in existing:
                conn.execute(f"ALTER TABLE live_orders ADD COLUMN {column_name} {column_type}")

    def _backfill_positions(self, conn: sqlite3.Connection) -> None:
        conn.execute("DELETE FROM positions WHERE status = 'open'")
        rows = conn.execute(
            """
            SELECT client_order_id
            FROM live_orders
            WHERE status IN ('accepted', 'resting', 'partial_fill', 'filled', 'live', 'open', 'submission_unconfirmed')
            ORDER BY first_seen_at ASC
            """
        ).fetchall()
        for row in rows:
            client_order_id = str(row["client_order_id"] or "")
            if client_order_id:
                self._sync_position_from_fills(conn, client_order_id)

    def init_run(
        self,
        *,
        run_id: str,
        generated_at: str,
        filters: dict[str, Any],
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO bot_runs (
                    run_id, generated_at, raw_count, count_selected, filters_json
                ) VALUES (?, ?, 0, 0, ?)
                """,
                (run_id, generated_at, _json_dumps(filters)),
            )

    def persist_run(
        self,
        *,
        run_id: str,
        generated_at: str,
        raw_count: int,
        count_selected: int,
        filters: dict[str, Any],
        raw_predictions: list[dict[str, Any]],
        opportunities: list[dict[str, Any]],
        order_plans: list[dict[str, Any]],
        executions: list[dict[str, Any]],
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO bot_runs (
                    run_id, generated_at, raw_count, count_selected, filters_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, generated_at, raw_count, count_selected, _json_dumps(filters)),
            )
            conn.execute("DELETE FROM opportunities WHERE run_id = ?", (run_id,))
            conn.execute("DELETE FROM scan_predictions WHERE run_id = ?", (run_id,))
            conn.execute("DELETE FROM order_plans WHERE run_id = ?", (run_id,))
            conn.execute("DELETE FROM executions WHERE run_id = ?", (run_id,))

            selected_rank_map = {
                f"{item['event_slug']}|{item['market_slug']}|{item['side']}": rank
                for rank, item in enumerate(opportunities, start=1)
            }

            for item in raw_predictions:
                key = f"{item['event_slug']}|{item['market_slug']}|{item['side']}"
                selected_rank = selected_rank_map.get(key)
                prediction_payload = {
                    "run_id": run_id,
                    "generated_at": generated_at,
                    "selected_rank": selected_rank,
                    "is_selected": 1 if selected_rank is not None else 0,
                    "city_key": item["city_key"],
                    "city_name": item["city_name"],
                    "day_label": item["day_label"],
                    "date_str": item["date_str"],
                    "event_slug": item["event_slug"],
                    "event_title": item["event_title"],
                    "bucket": item["bucket"],
                    "side": item["side"],
                    "edge": item["edge"],
                    "ev_percent": item["ev_percent"],
                    "price_cents": item["price_cents"],
                    "model_prob": item["model_prob"],
                    "market_prob": item["market_prob"],
                    "ensemble_prediction": item["ensemble_prediction"],
                    "weighted_score": item["weighted_score"],
                    "consensus_score": item["consensus_score"],
                    "spread": item["spread"],
                    "sigma": item["sigma"],
                    "agreement_models": item.get("agreement_models", 0),
                    "total_models": item.get("total_models", 0),
                    "agreement_pct": item.get("agreement_pct", 0.0),
                    "confidence_tier": item.get("confidence_tier", "risky"),
                    "coverage_ok": 1 if item.get("coverage_ok") else 0,
                    "coverage_score": item.get("coverage_score", 0.0),
                    "coverage_issue_type": item.get("coverage_issue_type"),
                    "degraded_reason": item.get("degraded_reason"),
                    "agreement_summary": item.get("agreement_summary"),
                    "signal_tier": item.get("signal_tier", "C"),
                    "signal_decision": item.get("signal_decision"),
                    "mean_agreeing_model_edge": item.get("mean_agreeing_model_edge", 0.0),
                    "min_agreeing_model_edge": item.get("min_agreeing_model_edge", 0.0),
                    "agreeing_model_count": item.get("agreeing_model_count", 0),
                    "executable_quality_score": item.get("executable_quality_score", 0.0),
                    "data_quality_score": item.get("data_quality_score", 0.0),
                    "valid_model_count": item.get("valid_model_count", 0),
                    "required_model_count": item.get("required_model_count", 0),
                    "provider_failures_json": _json_dumps(item.get("provider_failures", [])),
                    "provider_failure_details_json": _json_dumps(item.get("provider_failure_details", {})),
                    "effective_weights_json": _json_dumps(item.get("effective_weights", {})),
                    "adversarial_score": item.get("adversarial_score", 0.0),
                    "execution_priority_score": item.get("execution_priority_score", 0.0),
                    "policy_allowed": 1 if item.get("policy_allowed") else 0,
                    "policy_reason": item.get("policy_reason"),
                    "token_id": item.get("token_id"),
                    "market_slug": item["market_slug"],
                    "market_id": item["market_id"],
                    "model_predictions_json": _json_dumps(item.get("model_predictions", {})),
                    "price_source": item.get("price_source"),
                    "reference_price_cents": item.get("reference_price_cents"),
                    "best_bid_cents": item.get("best_bid_cents"),
                    "best_ask": item.get("best_ask"),
                    "last_trade_price": item.get("last_trade_price"),
                    "order_min_size": item.get("order_min_size"),
                    "polymarket_url": item["polymarket_url"],
                }
                prediction_columns = list(prediction_payload.keys())
                conn.execute(
                    f"INSERT INTO scan_predictions ({', '.join(prediction_columns)}) VALUES ({', '.join(['?'] * len(prediction_columns))})",
                    tuple(prediction_payload[column] for column in prediction_columns),
                )

            for rank, item in enumerate(opportunities, start=1):
                opportunity_payload = {
                    "run_id": run_id,
                    "rank": rank,
                    "city_key": item["city_key"],
                    "city_name": item["city_name"],
                    "day_label": item["day_label"],
                    "date_str": item["date_str"],
                    "event_slug": item["event_slug"],
                    "event_title": item["event_title"],
                    "bucket": item["bucket"],
                    "side": item["side"],
                    "edge": item["edge"],
                    "ev_percent": item["ev_percent"],
                    "price_cents": item["price_cents"],
                    "model_prob": item["model_prob"],
                    "market_prob": item["market_prob"],
                    "ensemble_prediction": item["ensemble_prediction"],
                    "weighted_score": item["weighted_score"],
                    "consensus_score": item["consensus_score"],
                    "spread": item["spread"],
                    "sigma": item["sigma"],
                    "token_id": item.get("token_id"),
                    "market_slug": item["market_slug"],
                    "market_id": item["market_id"],
                    "best_ask": item.get("best_ask"),
                    "last_trade_price": item.get("last_trade_price"),
                    "order_min_size": item.get("order_min_size"),
                    "agreement_models": item.get("agreement_models", 0),
                    "total_models": item.get("total_models", 0),
                    "agreement_pct": item.get("agreement_pct", 0.0),
                    "agreement_summary": item.get("agreement_summary"),
                    "confidence_tier": item.get("confidence_tier", "risky"),
                    "signal_tier": item.get("signal_tier", "C"),
                    "signal_decision": item.get("signal_decision"),
                    "coverage_ok": 1 if item.get("coverage_ok") else 0,
                    "coverage_score": item.get("coverage_score", 0.0),
                    "coverage_issue_type": item.get("coverage_issue_type"),
                    "degraded_reason": item.get("degraded_reason"),
                    "mean_agreeing_model_edge": item.get("mean_agreeing_model_edge", 0.0),
                    "min_agreeing_model_edge": item.get("min_agreeing_model_edge", 0.0),
                    "agreeing_model_count": item.get("agreeing_model_count", 0),
                    "executable_quality_score": item.get("executable_quality_score", 0.0),
                    "data_quality_score": item.get("data_quality_score", 0.0),
                    "valid_model_count": item.get("valid_model_count", 0),
                    "required_model_count": item.get("required_model_count", 0),
                    "provider_failures_json": _json_dumps(item.get("provider_failures", [])),
                    "provider_failure_details_json": _json_dumps(item.get("provider_failure_details", {})),
                    "effective_weights_json": _json_dumps(item.get("effective_weights", {})),
                    "adversarial_score": item.get("adversarial_score", 0.0),
                    "execution_priority_score": item.get("execution_priority_score", 0.0),
                    "policy_allowed": 1 if item.get("policy_allowed") else 0,
                    "policy_reason": item.get("policy_reason"),
                    "model_predictions_json": _json_dumps(item.get("model_predictions", {})),
                    "polymarket_url": item["polymarket_url"],
                    "price_source": item.get("price_source"),
                    "reference_price_cents": item.get("reference_price_cents"),
                    "best_bid_cents": item.get("best_bid_cents"),
                }
                opportunity_columns = list(opportunity_payload.keys())
                conn.execute(
                    f"INSERT INTO opportunities ({', '.join(opportunity_columns)}) VALUES ({', '.join(['?'] * len(opportunity_columns))})",
                    tuple(opportunity_payload[column] for column in opportunity_columns),
                )

            for rank, item in enumerate(order_plans, start=1):
                conn.execute(
                    """
                    INSERT INTO order_plans (
                        run_id, rank, event_slug, market_slug, side, token_id,
                        limit_price_cents, model_prob, market_prob, edge, ev_percent,
                        confidence_tier, bankroll_usd, stake_fraction, stake_usd, share_size, polymarket_url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        rank,
                        item["event_slug"],
                        item["market_slug"],
                        item["side"],
                        item.get("token_id"),
                        item["limit_price_cents"],
                        item["model_prob"],
                        item["market_prob"],
                        item["edge"],
                        item["ev_percent"],
                        item["confidence_tier"],
                        item["bankroll_usd"],
                        item["stake_fraction"],
                        item["stake_usd"],
                        item["share_size"],
                        item["polymarket_url"],
                    ),
                )

            for rank, item in enumerate(executions, start=1):
                conn.execute(
                    """
                    INSERT INTO executions (
                        run_id, rank, mode, success, accepted, market_slug, event_slug, side, token_id,
                        client_order_id, nonce, submission_fingerprint, exchange_order_id, order_status, filled_shares, avg_fill_price_cents,
                        price_cents, share_size, error, response_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        rank,
                        item["mode"],
                        1 if item["success"] else 0,
                        1 if item.get("accepted") else 0,
                        item["market_slug"],
                        item.get("event_slug"),
                        item["side"],
                        item.get("token_id"),
                        item.get("client_order_id"),
                        item.get("nonce"),
                        item.get("submission_fingerprint"),
                        item.get("exchange_order_id"),
                        item.get("order_status"),
                        item.get("filled_shares"),
                        item.get("avg_fill_price_cents"),
                        item["price_cents"],
                        item["share_size"],
                        item.get("error"),
                        _json_dumps(item.get("response", {})),
                    ),
                )

    def append_live_execution(
        self,
        *,
        run_id: str,
        generated_at: str,
        rank: int,
        opportunity: dict[str, Any],
        plan: dict[str, Any],
        execution: dict[str, Any],
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO executions (
                    run_id, rank, mode, success, accepted, market_slug, event_slug, side, token_id,
                    client_order_id, nonce, submission_fingerprint, exchange_order_id, order_status, filled_shares, avg_fill_price_cents,
                    price_cents, share_size, error, response_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    rank,
                    execution["mode"],
                    1 if execution["success"] else 0,
                    1 if execution.get("accepted") else 0,
                    execution["market_slug"],
                    execution.get("event_slug") or opportunity.get("event_slug"),
                    execution["side"],
                    execution.get("token_id") or opportunity.get("token_id"),
                    execution.get("client_order_id"),
                    execution.get("nonce"),
                    execution.get("submission_fingerprint"),
                    execution.get("exchange_order_id"),
                    execution.get("order_status"),
                    execution.get("filled_shares"),
                    execution.get("avg_fill_price_cents"),
                    execution["price_cents"],
                    execution["share_size"],
                    execution.get("error"),
                    _json_dumps(execution.get("response", {})),
                ),
            )
            self._upsert_live_order_and_fills(
                conn,
                run_id,
                rank,
                execution,
                generated_at,
                opportunity=opportunity,
                plan=plan,
            )

    def _upsert_live_order_and_fills(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        rank: int,
        execution: dict[str, Any],
        generated_at: str,
        opportunity: dict[str, Any] | None = None,
        plan: dict[str, Any] | None = None,
    ) -> None:
        if execution.get("mode") != "live":
            return
        client_order_id = execution.get("client_order_id")
        if not client_order_id:
            return
        plan_row = plan
        opp_row = opportunity
        if plan_row is None:
            row = conn.execute(
                """
                SELECT event_slug, market_slug, side, token_id, limit_price_cents, share_size, stake_usd,
                       tick_size_cents, order_min_size, polymarket_url
                FROM order_plans
                WHERE run_id = ? AND rank = ?
                """,
                (run_id, rank),
            ).fetchone()
            plan_row = _row_to_dict(row) if row is not None else None
        if opp_row is None:
            row = conn.execute(
                """
                SELECT city_key, date_str, polymarket_url, order_min_size, event_slug, market_slug, side, token_id
                FROM opportunities
                WHERE run_id = ? AND rank = ?
                """,
                (run_id, rank),
            ).fetchone()
            opp_row = _row_to_dict(row) if row is not None else None
        if plan_row is None:
            return
        conn.execute(
            """
            INSERT INTO live_orders (
                client_order_id, exchange_order_id, run_id, rank, event_slug, market_slug, city_key, date_str, side, token_id,
                nonce, submission_fingerprint, polymarket_url, requested_price_cents, requested_shares, filled_shares, avg_fill_price_cents, status, accepted,
                order_min_size, tick_size_cents, first_seen_at, last_seen_at, raw_response_json, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(client_order_id) DO UPDATE SET
                exchange_order_id = excluded.exchange_order_id,
                filled_shares = excluded.filled_shares,
                avg_fill_price_cents = excluded.avg_fill_price_cents,
                status = excluded.status,
                accepted = excluded.accepted,
                last_seen_at = excluded.last_seen_at,
                raw_response_json = excluded.raw_response_json,
                error = excluded.error
            """,
            (
                client_order_id,
                execution.get("exchange_order_id"),
                run_id,
                rank,
                execution.get("event_slug") or plan_row["event_slug"],
                execution.get("market_slug") or plan_row["market_slug"],
                (opp_row or {}).get("city_key"),
                (opp_row or {}).get("date_str"),
                execution.get("side") or plan_row["side"],
                execution.get("token_id") or plan_row["token_id"],
                execution.get("nonce"),
                execution.get("submission_fingerprint"),
                (opp_row or {}).get("polymarket_url") or plan_row.get("polymarket_url"),
                execution.get("price_cents") or plan_row["limit_price_cents"],
                execution.get("share_size") or plan_row["share_size"],
                execution.get("filled_shares") or 0.0,
                execution.get("avg_fill_price_cents"),
                execution.get("order_status") or "unknown",
                1 if execution.get("accepted") else 0,
                (opp_row or {}).get("order_min_size") or plan_row.get("order_min_size"),
                plan_row.get("tick_size_cents"),
                generated_at,
                generated_at,
                _json_dumps(execution.get("response", {})),
                execution.get("error"),
            ),
        )
        for fill in execution.get("fills", []) or []:
            fill_id = str(fill.get("id") or "") or None
            share_size = _safe_float(fill.get("share_size")) or 0.0
            fill_price_cents = _safe_float(fill.get("price_cents")) or 0.0
            if share_size <= 0 or fill_price_cents <= 0:
                continue
            conn.execute(
                """
                INSERT OR IGNORE INTO live_fills (
                    fill_id, client_order_id, exchange_order_id, run_id, rank, event_slug, market_slug, side, token_id,
                    share_size, fill_price_cents, notional_usd, filled_at, raw_fill_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                        fill_id,
                        client_order_id,
                        execution.get("exchange_order_id"),
                        run_id,
                        rank,
                        execution.get("event_slug") or plan_row["event_slug"],
                        execution.get("market_slug") or plan_row["market_slug"],
                        execution.get("side") or plan_row["side"],
                        execution.get("token_id") or plan_row["token_id"],
                        share_size,
                        fill_price_cents,
                        round(share_size * fill_price_cents / 100.0, 4),
                    fill.get("timestamp") or generated_at,
                    _json_dumps(fill),
                ),
            )
        self._sync_position_from_fills(conn, client_order_id)

    def _sync_position_from_fills(self, conn: sqlite3.Connection, client_order_id: str) -> None:
        order_row = conn.execute(
            """
            SELECT *
            FROM live_orders
            WHERE client_order_id = ?
            """,
            (client_order_id,),
        ).fetchone()
        if order_row is None:
            return
        fill_rows = conn.execute(
            """
            SELECT share_size, fill_price_cents, notional_usd
            FROM live_fills
            WHERE client_order_id = ?
            """,
            (client_order_id,),
        ).fetchall()
        total_shares = sum(float(row["share_size"]) for row in fill_rows)
        total_notional = sum(float(row["notional_usd"]) for row in fill_rows)
        if total_shares <= 0:
            conn.execute(
                "DELETE FROM positions WHERE client_order_id = ? AND status = 'open'",
                (client_order_id,),
            )
            return
        avg_fill_price_cents = round((total_notional / total_shares) * 100.0, 4) if total_notional > 0 else None
        existing = conn.execute(
            "SELECT id, status FROM positions WHERE client_order_id = ?",
            (client_order_id,),
        ).fetchone()
        opened_at = str(order_row["first_seen_at"])
        if existing is None:
            conn.execute(
                """
                INSERT INTO positions (
                    run_id, rank, mode, client_order_id, exchange_order_id, event_slug, market_slug, city_key, date_str,
                    side, token_id, entry_price_cents, share_size, stake_usd, status, opened_at, polymarket_url
                ) VALUES (?, ?, 'live', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)
                """,
                (
                    order_row["run_id"],
                    order_row["rank"],
                    client_order_id,
                    order_row["exchange_order_id"],
                    order_row["event_slug"],
                    order_row["market_slug"],
                    order_row["city_key"] or "",
                    order_row["date_str"] or "",
                    order_row["side"],
                    order_row["token_id"],
                    avg_fill_price_cents or float(order_row["requested_price_cents"]),
                    round(total_shares, 4),
                    round(total_notional, 4),
                    opened_at,
                    order_row["polymarket_url"] or f"https://polymarket.com/event/{order_row['event_slug']}",
                ),
            )
            return
        if str(existing["status"]) == "resolved":
            return
        conn.execute(
            """
            UPDATE positions
            SET exchange_order_id = ?,
                entry_price_cents = ?,
                share_size = ?,
                stake_usd = ?
            WHERE id = ?
            """,
            (
                order_row["exchange_order_id"],
                avg_fill_price_cents or float(order_row["requested_price_cents"]),
                round(total_shares, 4),
                round(total_notional, 4),
                existing["id"],
            ),
        )

    def sync_position_resolution(
        self,
        *,
        event_slug: str,
        market_slug: str,
        side: str,
        settled_price_cents: float,
        resolution_source: str | None,
        resolved_by: str | None,
        resolved_at: str,
        notes: str | None = None,
    ) -> int:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, entry_price_cents, share_size, stake_usd
                FROM positions
                WHERE event_slug = ? AND market_slug = ? AND side = ? AND mode = 'live' AND status != 'resolved'
                """,
                (event_slug, market_slug, side),
            ).fetchall()
            updated = 0
            for row in rows:
                share_size = float(row["share_size"])
                payout_usd = round(share_size * (settled_price_cents / 100.0), 4)
                pnl_usd = round(payout_usd - float(row["stake_usd"]), 4)
                stake_usd = float(row["stake_usd"])
                roi_percent = round((pnl_usd / stake_usd) * 100.0, 4) if stake_usd > 0 else None
                conn.execute(
                    """
                    UPDATE positions
                    SET status = 'resolved',
                        settled_price_cents = ?,
                        payout_usd = ?,
                        pnl_usd = ?,
                        roi_percent = ?,
                        resolution_source = ?,
                        resolved_by = ?,
                        resolved_at = ?,
                        notes = ?
                    WHERE id = ?
                    """,
                    (
                        settled_price_cents,
                        payout_usd,
                        pnl_usd,
                        roi_percent,
                        resolution_source,
                        resolved_by,
                        resolved_at,
                        notes,
                        row["id"],
                    ),
                )
                updated += 1
            return updated

    def sync_prediction_resolution(
        self,
        *,
        event_slug: str,
        market_slug: str,
        side: str,
        settled_price_cents: float,
        resolution_source: str | None,
        resolved_by: str | None,
        resolved_at: str,
    ) -> int:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, price_cents
                FROM scan_predictions
                WHERE event_slug = ? AND market_slug = ? AND side = ?
                  AND resolved_at IS NULL
                """,
                (event_slug, market_slug, side),
            ).fetchall()
            updated = 0
            for row in rows:
                stake_usd = 1.0
                entry_price_cents = _safe_float(row["price_cents"]) or 0.0
                payout_usd = stake_usd * (settled_price_cents / max(entry_price_cents, 0.0001)) if entry_price_cents > 0 else 0.0
                pnl_usd = round(payout_usd - stake_usd, 4)
                roi_percent = round((pnl_usd / stake_usd) * 100.0, 4) if stake_usd > 0 else None
                conn.execute(
                    """
                    UPDATE scan_predictions
                    SET settled_price_cents = ?,
                        pnl_usd = ?,
                        roi_percent = ?,
                        resolution_source = ?,
                        resolved_by = ?,
                        resolved_at = ?
                    WHERE id = ?
                    """,
                    (
                        settled_price_cents,
                        pnl_usd,
                        roi_percent,
                        resolution_source,
                        resolved_by,
                        resolved_at,
                        row["id"],
                    ),
                )
                updated += 1
            return updated

    def list_unresolved_prediction_markets(self, limit: int = 500) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT event_slug, market_slug, side
                FROM scan_predictions
                WHERE resolved_at IS NULL
                GROUP BY event_slug, market_slug, side
                ORDER BY MAX(generated_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def list_open_positions(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM positions
                WHERE mode = 'live' AND status = 'open'
                ORDER BY opened_at ASC, id ASC
                """
            ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def list_live_orders(
        self,
        statuses: tuple[str, ...] | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            if statuses:
                placeholders = ",".join("?" for _ in statuses)
                rows = conn.execute(
                    f"""
                    SELECT *
                    FROM live_orders
                    WHERE status IN ({placeholders})
                    ORDER BY last_seen_at DESC, id DESC
                    LIMIT ? OFFSET ?
                    """,
                    (*statuses, limit, offset),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM live_orders
                    ORDER BY last_seen_at DESC, id DESC
                    LIMIT ? OFFSET ?
                    """,
                    (limit, offset),
                ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def sync_live_order_state(
        self,
        *,
        client_order_id: str,
        exchange_order_id: str | None,
        status: str,
        accepted: bool,
        filled_shares: float,
        avg_fill_price_cents: float | None,
        response: dict[str, Any],
        fills: list[dict[str, Any]],
        synced_at: str,
        error: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE live_orders
                SET exchange_order_id = COALESCE(?, exchange_order_id),
                    status = ?,
                    accepted = ?,
                    filled_shares = ?,
                    avg_fill_price_cents = ?,
                    last_seen_at = ?,
                    raw_response_json = ?,
                    error = ?
                WHERE client_order_id = ?
                """,
                (
                    exchange_order_id,
                    status,
                    1 if accepted else 0,
                    filled_shares,
                    avg_fill_price_cents,
                    synced_at,
                    _json_dumps(response),
                    error,
                    client_order_id,
                ),
            )
            order_row = conn.execute(
                "SELECT run_id, rank, event_slug, market_slug, side, token_id FROM live_orders WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
            if order_row is None:
                return
            for fill in fills:
                fill_id = str(fill.get("id") or "") or None
                share_size = _safe_float(fill.get("share_size")) or 0.0
                fill_price_cents = _safe_float(fill.get("price_cents")) or 0.0
                if share_size <= 0 or fill_price_cents <= 0:
                    continue
                conn.execute(
                    """
                    INSERT OR IGNORE INTO live_fills (
                        fill_id, client_order_id, exchange_order_id, run_id, rank, event_slug, market_slug, side, token_id,
                        share_size, fill_price_cents, notional_usd, filled_at, raw_fill_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        fill_id,
                        client_order_id,
                        exchange_order_id,
                        order_row["run_id"],
                        order_row["rank"],
                        order_row["event_slug"],
                        order_row["market_slug"],
                        order_row["side"],
                        order_row["token_id"],
                        share_size,
                        fill_price_cents,
                        round(share_size * fill_price_cents / 100.0, 4),
                        fill.get("timestamp") or synced_at,
                        _json_dumps(fill),
                    ),
                )
            self._sync_position_from_fills(conn, client_order_id)

    def list_positions(self, limit: int = 200) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM positions
                ORDER BY opened_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def position_summary(self) -> dict[str, Any]:
        with self._connect() as conn:
            metrics = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_positions,
                    SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_positions,
                    SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) AS resolved_positions,
                    ROUND(COALESCE(SUM(CASE WHEN status = 'open' THEN stake_usd ELSE 0 END), 0), 4) AS open_exposure_usd,
                    ROUND(COALESCE(SUM(CASE WHEN status = 'resolved' THEN pnl_usd ELSE 0 END), 0), 4) AS realized_pnl_usd,
                    ROUND(COALESCE(AVG(CASE WHEN status = 'resolved' THEN roi_percent END), 0), 4) AS avg_roi_percent,
                    SUM(CASE WHEN status = 'resolved' AND pnl_usd > 0 THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN status = 'resolved' AND pnl_usd < 0 THEN 1 ELSE 0 END) AS losses
                FROM positions
                """
            ).fetchone()
            bankroll_row = conn.execute(
                """
                SELECT bankroll_usd
                FROM order_plans
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            city_pnl = conn.execute(
                """
                SELECT city_key, COUNT(*) AS qty, ROUND(COALESCE(SUM(pnl_usd), 0), 4) AS pnl_usd
                FROM positions
                WHERE status = 'resolved'
                GROUP BY city_key
                ORDER BY pnl_usd DESC, qty DESC
                LIMIT 10
                """
            ).fetchall()
            pnl_curve = conn.execute(
                """
                SELECT
                    resolved_at,
                    ROUND(COALESCE(SUM(pnl_usd), 0), 4) AS pnl_usd
                FROM positions
                WHERE status = 'resolved' AND resolved_at IS NOT NULL
                GROUP BY resolved_at
                ORDER BY resolved_at ASC
                """
            ).fetchall()
        metrics_dict = _row_to_dict(metrics) if metrics else {}
        latest_bankroll_usd = _safe_float(bankroll_row["bankroll_usd"]) if bankroll_row is not None else None
        if latest_bankroll_usd is not None:
            metrics_dict["latest_bankroll_usd"] = latest_bankroll_usd
            metrics_dict["estimated_available_balance_usd"] = round(
                latest_bankroll_usd
                + float(metrics_dict.get("realized_pnl_usd") or 0.0)
                - float(metrics_dict.get("open_exposure_usd") or 0.0),
                4,
            )
        return {
            "metrics": metrics_dict,
            "city_pnl": [_row_to_dict(row) for row in city_pnl],
            "pnl_curve": [_row_to_dict(row) for row in pnl_curve],
        }

    def prediction_summary(self) -> dict[str, Any]:
        with self._connect() as conn:
            overall = conn.execute(
                """
                SELECT
                    COUNT(*) AS resolved_predictions,
                    SUM(CASE WHEN settled_price_cents > 0 THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN settled_price_cents = 0 THEN 1 ELSE 0 END) AS losses,
                    ROUND(COALESCE(SUM(pnl_usd), 0), 4) AS total_pnl_usd
                FROM scan_predictions
                WHERE settled_price_cents IS NOT NULL
                """
            ).fetchone()
            allowed = conn.execute(
                """
                SELECT
                    COUNT(*) AS resolved_predictions,
                    SUM(CASE WHEN settled_price_cents > 0 THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN settled_price_cents = 0 THEN 1 ELSE 0 END) AS losses,
                    ROUND(COALESCE(SUM(pnl_usd), 0), 4) AS total_pnl_usd
                FROM scan_predictions
                WHERE settled_price_cents IS NOT NULL
                  AND policy_allowed = 1
                """
            ).fetchone()

        def _build_metrics(row: sqlite3.Row | None) -> dict[str, Any]:
            if row is None:
                return {
                    "resolved_predictions": 0,
                    "wins": 0,
                    "losses": 0,
                    "total_pnl_usd": 0.0,
                    "win_rate_percent": None,
                }
            resolved = int(row["resolved_predictions"] or 0)
            wins = int(row["wins"] or 0)
            losses = int(row["losses"] or 0)
            total_pnl = _safe_float(row["total_pnl_usd"]) or 0.0
            return {
                "resolved_predictions": resolved,
                "wins": wins,
                "losses": losses,
                "total_pnl_usd": round(total_pnl, 4),
                "win_rate_percent": round((wins / resolved) * 100.0, 4) if resolved > 0 else None,
            }

        return {
            "overall": _build_metrics(overall),
            "policy_allowed": _build_metrics(allowed),
        }

    def recent_opportunity_ranges(
        self,
        keys: list[tuple[str, str]],
        *,
        limit_runs: int = 50,
    ) -> dict[str, dict[str, Any]]:
        if not keys:
            return {}

        clauses: list[str] = []
        params: list[Any] = []
        for market_slug, side in keys:
            clauses.append("(market_slug = ? AND side = ?)")
            params.extend([market_slug, side])
        query = f"""
            WITH recent_runs AS (
                SELECT run_id
                FROM bot_runs
                ORDER BY generated_at DESC
                LIMIT ?
            )
            SELECT
                market_slug,
                side,
                ROUND(MIN(price_cents), 4) AS min_price_cents,
                ROUND(MAX(price_cents), 4) AS max_price_cents,
                COUNT(*) AS samples
            FROM opportunities
            WHERE run_id IN (SELECT run_id FROM recent_runs)
              AND ({' OR '.join(clauses)})
            GROUP BY market_slug, side
        """

        final_params = [limit_runs, *params]
        with self._connect() as conn:
            rows = conn.execute(query, final_params).fetchall()

        output: dict[str, dict[str, Any]] = {}
        for row in rows:
            key = f"{row['market_slug']}|{row['side']}"
            output[key] = _row_to_dict(row)
        return output

    def list_runs(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT run_id, generated_at, raw_count, count_selected, filters_json, created_at
                FROM bot_runs
                ORDER BY generated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        output = [_row_to_dict(row) for row in rows]
        for item in output:
            item["filters"] = json.loads(item.pop("filters_json"))
        return output

    def get_run_details(self, run_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            run = conn.execute(
                """
                SELECT run_id, generated_at, raw_count, count_selected, filters_json, created_at
                FROM bot_runs WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            if run is None:
                raise KeyError(run_id)
            opportunities = conn.execute(
                """
                SELECT * FROM opportunities
                WHERE run_id = ?
                ORDER BY rank ASC
                """,
                (run_id,),
            ).fetchall()
            raw_predictions = conn.execute(
                """
                SELECT * FROM scan_predictions
                WHERE run_id = ?
                ORDER BY is_selected DESC, selected_rank ASC, edge DESC
                """,
                (run_id,),
            ).fetchall()
            order_plans = conn.execute(
                """
                SELECT * FROM order_plans
                WHERE run_id = ?
                ORDER BY rank ASC
                """,
                (run_id,),
            ).fetchall()
            executions = conn.execute(
                """
                SELECT * FROM executions
                WHERE run_id = ?
                ORDER BY rank ASC
                """,
                (run_id,),
            ).fetchall()
            positions = conn.execute(
                """
                SELECT * FROM positions
                WHERE run_id = ?
                ORDER BY rank ASC
                """,
                (run_id,),
            ).fetchall()

        run_dict = _row_to_dict(run)
        run_dict["filters"] = json.loads(run_dict.pop("filters_json"))

        opportunity_dicts = [_row_to_dict(row) for row in opportunities]
        for item in opportunity_dicts:
            item["model_predictions"] = json.loads(item.pop("model_predictions_json"))
        raw_prediction_dicts = [_row_to_dict(row) for row in raw_predictions]
        for item in raw_prediction_dicts:
            item["model_predictions"] = json.loads(item.pop("model_predictions_json"))
        plan_dicts = [_row_to_dict(row) for row in order_plans]
        execution_dicts = [_row_to_dict(row) for row in executions]
        for item in execution_dicts:
            item["response"] = json.loads(item.pop("response_json"))
            item["success"] = bool(item["success"])
        position_dicts = [_row_to_dict(row) for row in positions]
        return {
            "run": run_dict,
            "opportunities": opportunity_dicts,
            "raw_predictions": raw_prediction_dicts,
            "order_plans": plan_dicts,
            "executions": execution_dicts,
            "positions": position_dicts,
        }

    def summary_metrics(self) -> dict[str, Any]:
        with self._connect() as conn:
            metrics = conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM bot_runs) AS total_runs,
                    (SELECT COUNT(*) FROM opportunities) AS total_opportunities,
                    (SELECT COUNT(*) FROM executions) AS total_executions,
                    (SELECT COUNT(*) FROM executions WHERE success = 1) AS successful_executions,
                    (SELECT generated_at FROM bot_runs ORDER BY generated_at DESC LIMIT 1) AS latest_run_at
                """
            ).fetchone()
            top_cities = conn.execute(
                """
                SELECT city_key, COUNT(*) AS qty, ROUND(AVG(edge), 2) AS avg_edge
                FROM opportunities
                GROUP BY city_key
                ORDER BY qty DESC, avg_edge DESC
                LIMIT 10
                """
            ).fetchall()
            recent_execs = conn.execute(
                """
                SELECT run_id, rank, mode, success, market_slug, side, price_cents, share_size, error
                FROM executions
                ORDER BY id DESC
                LIMIT 50
                """
            ).fetchall()
            position_metrics = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_positions,
                    SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_positions,
                    SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) AS resolved_positions,
                    ROUND(COALESCE(SUM(CASE WHEN status = 'resolved' THEN pnl_usd ELSE 0 END), 0), 4) AS realized_pnl_usd,
                    SUM(CASE WHEN status = 'resolved' AND pnl_usd > 0 THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN status = 'resolved' AND pnl_usd < 0 THEN 1 ELSE 0 END) AS losses
                FROM positions
                """
            ).fetchone()

        output_metrics = _row_to_dict(metrics) if metrics else {}
        if position_metrics is not None:
            output_metrics.update(_row_to_dict(position_metrics))

        return {
            "metrics": output_metrics,
            "top_cities": [_row_to_dict(row) for row in top_cities],
            "recent_executions": [_row_to_dict(row) for row in recent_execs],
        }

    def record_live_account_snapshot(
        self,
        *,
        captured_at: str,
        saldo_usd: float | None,
        portfolio_usd: float | None,
        total_net_worth_usd: float | None,
        total_open_pnl_usd: float | None,
        open_positions_count: int,
        min_interval_seconds: int = 20,
    ) -> bool:
        with self._connect() as conn:
            latest = conn.execute(
                """
                SELECT captured_at
                FROM live_account_snapshots
                ORDER BY captured_at DESC
                LIMIT 1
                """
            ).fetchone()
            if latest is not None:
                latest_ts = str(latest["captured_at"])
                try:
                    from datetime import datetime

                    latest_dt = datetime.fromisoformat(latest_ts.replace("Z", "+00:00"))
                    current_dt = datetime.fromisoformat(captured_at.replace("Z", "+00:00"))
                    if (current_dt - latest_dt).total_seconds() < min_interval_seconds:
                        return False
                except Exception:
                    pass
            conn.execute(
                """
                INSERT INTO live_account_snapshots (
                    captured_at, saldo_usd, portfolio_usd, total_net_worth_usd,
                    total_open_pnl_usd, open_positions_count
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    captured_at,
                    saldo_usd,
                    portfolio_usd,
                    total_net_worth_usd,
                    total_open_pnl_usd,
                    open_positions_count,
                ),
            )
            return True

    def list_live_account_snapshots(self, limit: int = 200) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM live_account_snapshots
                ORDER BY captured_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def list_recent_market_targets(
        self,
        *,
        limit: int = 100,
        lookback_days: int = 7,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                WITH recent AS (
                    SELECT
                        city_key,
                        date_str,
                        event_slug,
                        MAX(generated_at) AS latest_generated_at
                    FROM scan_predictions
                    WHERE generated_at >= datetime('now', ?)
                    GROUP BY city_key, date_str, event_slug
                )
                SELECT city_key, date_str, event_slug, latest_generated_at
                FROM recent
                ORDER BY latest_generated_at DESC
                LIMIT ?
                """,
                (f"-{max(1, int(lookback_days))} days", limit),
            ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def record_market_history_snapshots(
        self,
        snapshots: list[dict[str, Any]],
    ) -> int:
        if not snapshots:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO market_history_snapshots (
                    captured_at, source, city_key, date_str, event_slug, event_title, market_slug, market_id,
                    bucket, token_id_yes, token_id_no, yes_price_cents, no_price_cents, yes_best_ask_cents,
                    no_best_ask_cents, yes_best_bid_cents, no_best_bid_cents, last_trade_price, order_min_size, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.get("captured_at"),
                        item.get("source") or "gamma_clob",
                        item.get("city_key"),
                        item.get("date_str"),
                        item.get("event_slug"),
                        item.get("event_title"),
                        item.get("market_slug"),
                        item.get("market_id"),
                        item.get("bucket"),
                        item.get("token_id_yes"),
                        item.get("token_id_no"),
                        item.get("yes_price_cents"),
                        item.get("no_price_cents"),
                        item.get("yes_best_ask_cents"),
                        item.get("no_best_ask_cents"),
                        item.get("yes_best_bid_cents"),
                        item.get("no_best_bid_cents"),
                        item.get("last_trade_price"),
                        item.get("order_min_size"),
                        _json_dumps(item.get("raw_json") or {}),
                    )
                    for item in snapshots
                ],
            )
        return len(snapshots)

    def list_market_history_snapshots(
        self,
        *,
        market_slug: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT *
            FROM market_history_snapshots
        """
        params: list[Any] = []
        if market_slug:
            query += " WHERE market_slug = ?"
            params.append(market_slug)
        query += " ORDER BY captured_at DESC, id DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        output = [_row_to_dict(row) for row in rows]
        for item in output:
            item["raw_json"] = json.loads(item.get("raw_json") or "{}")
        return output

    def record_forecast_source_snapshots(self, snapshots: list[dict[str, Any]]) -> int:
        if not snapshots:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO forecast_source_snapshots (
                    run_id, captured_at, city_key, city_name, day_label, date_str, event_slug, market_slug,
                    market_id, bucket, side, source_name, forecast_temp_f, effective_weight, agreement_models,
                    total_models, agreement_pct, aligns_with_trade_side, source_in_bucket, source_delta_f,
                    raw_context_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.get("run_id"),
                        item.get("captured_at"),
                        item.get("city_key"),
                        item.get("city_name"),
                        item.get("day_label"),
                        item.get("date_str"),
                        item.get("event_slug"),
                        item.get("market_slug"),
                        item.get("market_id"),
                        item.get("bucket"),
                        item.get("side"),
                        item.get("source_name"),
                        item.get("forecast_temp_f"),
                        item.get("effective_weight"),
                        item.get("agreement_models"),
                        item.get("total_models"),
                        item.get("agreement_pct"),
                        1 if item.get("aligns_with_trade_side") else 0,
                        1 if item.get("source_in_bucket") else 0,
                        item.get("source_delta_f"),
                        _json_dumps(item.get("raw_context") or {}),
                    )
                    for item in snapshots
                ],
            )
        return len(snapshots)

    def list_forecast_source_snapshots(
        self,
        *,
        run_id: str | None = None,
        city_key: str | None = None,
        source_name: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT *
            FROM forecast_source_snapshots
        """
        clauses: list[str] = []
        params: list[Any] = []
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        if city_key:
            clauses.append("city_key = ?")
            params.append(city_key)
        if source_name:
            clauses.append("source_name = ?")
            params.append(source_name)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY captured_at DESC, id DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        output = [_row_to_dict(row) for row in rows]
        for item in output:
            item["raw_context"] = json.loads(item.pop("raw_context_json", "{}") or "{}")
            item["aligns_with_trade_side"] = bool(item.get("aligns_with_trade_side"))
            item["source_in_bucket"] = bool(item.get("source_in_bucket"))
        return output

    def record_station_observation_daily_highs(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO station_observation_daily_highs (
                    captured_at, city_key, city_name, station_id, local_date, observed_high_f, source, raw_context_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.get("captured_at"),
                        item.get("city_key"),
                        item.get("city_name"),
                        item.get("station_id"),
                        item.get("local_date"),
                        item.get("observed_high_f"),
                        item.get("source") or "nws_station_observation",
                        _json_dumps(item.get("raw_context") or {}),
                    )
                    for item in rows
                ],
            )
        return len(rows)

    def list_station_observation_daily_highs(
        self,
        *,
        city_key: str | None = None,
        local_date: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT *
            FROM station_observation_daily_highs
        """
        clauses: list[str] = []
        params: list[Any] = []
        if city_key:
            clauses.append("city_key = ?")
            params.append(city_key)
        if local_date:
            clauses.append("local_date = ?")
            params.append(local_date)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY captured_at DESC, id DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        output = [_row_to_dict(row) for row in rows]
        for item in output:
            item["raw_context"] = json.loads(item.pop("raw_context_json", "{}") or "{}")
        return output

    def forecast_accuracy_summary(
        self,
        *,
        min_samples: int = 5,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    f.source_name,
                    COUNT(*) AS sample_count,
                    ROUND(AVG(ABS(f.forecast_temp_f - s.observed_high_f)), 4) AS mae,
                    AVG((f.forecast_temp_f - s.observed_high_f) * (f.forecast_temp_f - s.observed_high_f)) AS mse_raw,
                    ROUND(AVG(f.forecast_temp_f - s.observed_high_f), 4) AS bias,
                    ROUND(AVG(CASE WHEN ABS(f.forecast_temp_f - s.observed_high_f) <= 1.0 THEN 1.0 ELSE 0.0 END), 4) AS within_1f_rate,
                    ROUND(AVG(CASE WHEN ABS(f.forecast_temp_f - s.observed_high_f) <= 2.0 THEN 1.0 ELSE 0.0 END), 4) AS within_2f_rate
                FROM forecast_source_snapshots f
                INNER JOIN station_observation_daily_highs s
                    ON s.city_key = f.city_key
                   AND s.local_date = f.date_str
                GROUP BY f.source_name
                HAVING COUNT(*) >= ?
                ORDER BY mae ASC, mse_raw ASC, ABS(bias) ASC, sample_count DESC
                LIMIT ?
                """,
                (max(1, int(min_samples)), max(1, int(limit))),
            ).fetchall()
        output: list[dict[str, Any]] = []
        for row in rows:
            item = _row_to_dict(row)
            mse_raw = _safe_float(item.pop("mse_raw"))
            item["rmse"] = round((mse_raw ** 0.5), 4) if mse_raw is not None and mse_raw >= 0 else None
            output.append(item)
        return output

    def policy_recommendations_summary(
        self,
        *,
        min_samples: int = 25,
        limit: int = 12,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    city_key,
                    day_label,
                    COUNT(*) AS sample_count,
                    SUM(CASE WHEN settled_price_cents >= 99.999 THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN settled_price_cents <= 0.001 THEN 1 ELSE 0 END) AS losses,
                    ROUND(AVG(COALESCE(pnl_usd, 0)), 4) AS avg_pnl_usd,
                    ROUND(SUM(COALESCE(pnl_usd, 0)), 4) AS total_pnl_usd,
                    ROUND(AVG(CASE WHEN settled_price_cents >= 99.999 THEN 1.0 ELSE 0.0 END), 4) AS win_rate,
                    ROUND(AVG(COALESCE(edge, 0)), 4) AS avg_edge,
                    ROUND(AVG(COALESCE(consensus_score, 0)), 4) AS avg_consensus,
                    ROUND(AVG(COALESCE(agreement_pct, 0)), 4) AS avg_agreement_pct
                FROM scan_predictions
                WHERE resolved_at IS NOT NULL
                GROUP BY city_key, day_label
                HAVING COUNT(*) >= ?
                ORDER BY sample_count DESC, avg_pnl_usd DESC
                LIMIT ?
                """,
                (max(1, int(min_samples)), max(1, int(limit * 3))),
            ).fetchall()
        output: list[dict[str, Any]] = []
        for row in rows:
            item = _row_to_dict(row)
            avg_pnl = _safe_float(item.get("avg_pnl_usd")) or 0.0
            win_rate = _safe_float(item.get("win_rate")) or 0.0
            city_key = str(item.get("city_key") or "").upper()
            day_label = str(item.get("day_label") or "").lower()
            if avg_pnl <= -0.5 or win_rate <= 0.10:
                recommendation = "block"
                rationale = "underperforming"
            elif avg_pnl < 0.0 or win_rate < 0.40:
                recommendation = "caution"
                rationale = "weak pnl_or_hit_rate"
            elif avg_pnl > 0.15 and win_rate >= 0.55:
                recommendation = "prefer"
                rationale = "strong pnl_and_hit_rate"
            else:
                recommendation = "neutral"
                rationale = "mixed"
            item["segment"] = f"{city_key}/{day_label}"
            item["recommendation"] = recommendation
            item["rationale"] = rationale
            output.append(item)
        recommendation_order = {"block": 0, "caution": 1, "neutral": 2, "prefer": 3}
        output.sort(
            key=lambda item: (
                recommendation_order.get(str(item.get("recommendation")), 99),
                -int(item.get("sample_count") or 0),
                float(item.get("avg_pnl_usd") or 0.0),
            )
        )
        return output[: max(1, int(limit))]
