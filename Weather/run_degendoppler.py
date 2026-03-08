from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from paperbot.degendoppler import scan_degendoppler_opportunities
from paperbot.polymarket_live import build_order_plan, summarize_plan


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Scan Degen Doppler weather signals and build Polymarket order plans.")
    parser.add_argument("--days-ahead", type=int, default=3, help="How many day buckets to scan: 1-3.")
    parser.add_argument("--min-edge", type=float, default=12.0, help="Minimum edge in percentage points.")
    parser.add_argument("--min-model-prob", type=float, default=15.0, help="Minimum model probability to keep.")
    parser.add_argument("--top", type=int, default=10, help="How many opportunities to print.")
    parser.add_argument("--bankroll", type=float, default=float(os.getenv("PAPERBOT_BANKROLL_USD", "1000")), help="Bankroll used for Kelly sizing.")
    parser.add_argument("--kelly-fraction", type=float, default=float(os.getenv("PAPERBOT_KELLY_FRACTION", "0.25")), help="Fractional Kelly multiplier.")
    parser.add_argument("--max-price-cents", type=float, default=None, help="Optional max entry price in cents.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of text.")
    parser.add_argument("--export-json", default=None, help="Optional JSON export path.")
    parser.add_argument("--export-csv", default=None, help="Optional CSV export path.")
    args = parser.parse_args(argv)

    opportunities = scan_degendoppler_opportunities(
        days_ahead=args.days_ahead,
        min_edge=args.min_edge,
        min_model_prob=args.min_model_prob,
    )
    selected = opportunities[: max(0, args.top)]
    plans = [
        build_order_plan(
            opportunity,
            bankroll_usd=args.bankroll,
            kelly_fraction=args.kelly_fraction,
            max_price_cents=args.max_price_cents,
        )
        for opportunity in selected
    ]

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(selected),
        "opportunities": [item.as_dict() for item in selected],
        "order_plans": [plan.as_dict() for plan in plans],
    }

    if args.export_json:
        _write_json(Path(args.export_json), payload)
    if args.export_csv:
        rows: list[dict] = []
        for opportunity, plan in zip(selected, plans):
            row = {**opportunity.as_dict(), **{f"plan_{k}": v for k, v in asdict(plan).items()}}
            rows.append(row)
        _write_csv(Path(args.export_csv), rows)

    if args.json:
        print(json.dumps(payload, indent=2))
        return

    if not selected:
        print("Nenhuma oportunidade encontrada com os filtros atuais.")
        return

    print(f"Oportunidades encontradas: {len(selected)}")
    for idx, (opportunity, plan) in enumerate(zip(selected, plans), start=1):
        print(
            f"{idx:02d}. {opportunity.city_key} {opportunity.date_str} {opportunity.side} {opportunity.bucket} "
            f"edge={opportunity.edge:.2f} model={opportunity.model_prob:.2f}% price={opportunity.price_cents:.2f}c"
        )
        print(f"    {summarize_plan(plan)}")
        print(f"    {plan.polymarket_url}")


if __name__ == "__main__":
    main()
