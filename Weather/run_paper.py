from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
from paperbot.main import main

PRESET_SIMULATION_ARGS: dict[str, list[str]] = {
    "maximo": [
        "--symbols", "BTCUSDC,ETHUSDC",
        "--feed-mode", "polymarket",
        "--ticks", "600",
        "--interval", "0.1",
        "--strategy-window", "2",
        "--strategy-threshold", "0.0000",
        "--strategy-size-bps", "0.01",
        "--risk-max-trade", "0.99",
        "--risk-max-daily-loss", "0.99",
        "--risk-max-symbol", "1000000",
        "--risk-max-total", "1000000",
        "--risk-min-score", "0.0",
        "--risk-stop", "0.999",
        "--risk-take", "0.999",
        "--paper-close-on-end",
    ],
    "controle_ruido": [
        "--symbols", "BTCUSDC,ETHUSDC",
        "--feed-mode", "polymarket",
        "--ticks", "600",
        "--interval", "0.2",
        "--strategy-window", "4",
        "--strategy-threshold", "0.0008",
        "--strategy-size-bps", "0.05",
        "--risk-max-trade", "0.05",
        "--risk-max-daily-loss", "0.12",
        "--risk-max-symbol", "350",
        "--risk-max-total", "900",
        "--risk-min-score", "0.30",
        "--risk-stop", "0.06",
        "--risk-take", "0.10",
        "--paper-close-on-end",
    ],
}

DEFAULT_PROFILE = "controle_ruido"
DEFAULT_SIMULATION_ARGS = PRESET_SIMULATION_ARGS[DEFAULT_PROFILE]


def _resolve_args(cli_args: list[str], selected_profile: str) -> list[str]:
    if cli_args:
        return cli_args
    if selected_profile in PRESET_SIMULATION_ARGS:
        return PRESET_SIMULATION_ARGS[selected_profile].copy()
    return PRESET_SIMULATION_ARGS[DEFAULT_PROFILE].copy()


def _find_arg_value(argv: list[str], name: str, default: str | None = None) -> str | None:
    for i, value in enumerate(argv):
        if value == name and i + 1 < len(argv):
            return argv[i + 1]
    return default


def _build_csv_path(export_dir: Path, argv: list[str]) -> Path:
    export_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    symbols = _find_arg_value(argv, "--symbols", "BTCUSDC,ETHUSDC").replace(",", "-")
    ticks = _find_arg_value(argv, "--ticks", "300")
    threshold = _find_arg_value(argv, "--strategy-threshold", "default").replace(".", "p")
    return export_dir / f"paper_{ts}_symbols-{symbols}_ticks-{ticks}_thr-{threshold}.csv"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run legacy paper bot simulation.")
    parser.add_argument("--export-dir", default="export", help="Diretorio para salvar CSV de history da simulacao.")
    parser.add_argument("--export-csv", default=None, help="Arquivo CSV de saida (opcional).")
    parser.add_argument("--no-export", action="store_true", help="Nao salvar relatorio CSV.")
    parser.add_argument("--profile", default=DEFAULT_PROFILE, choices=sorted(PRESET_SIMULATION_ARGS.keys()), help="Perfil predefinido de simulacao.")
    parser.add_argument("--use-defaults", action="store_true", help="Forca uso dos valores padrao do arquivo.")

    args, simulation_argv = parser.parse_known_args()
    simulation_argv = _resolve_args(
        simulation_argv if not args.use_defaults else PRESET_SIMULATION_ARGS[args.profile],
        selected_profile=args.profile,
    )
    if args.no_export:
        main(simulation_argv)
    else:
        export_csv = args.export_csv or str(
            _build_csv_path(Path(ROOT) / args.export_dir, simulation_argv)
        )
        os.environ["PAPERBOT_EXPORT_CSV"] = export_csv
        try:
            main(simulation_argv)
        finally:
            os.environ.pop("PAPERBOT_EXPORT_CSV", None)
