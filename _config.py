from __future__ import annotations
import os
import json
from pathlib import Path

_DEFAULT_OUTPUT = "/flashdata/llanteigne/earning_data/"
_DEFAULTS = {
    "USE_MULTIPROCESSING": False,
    "MAX_WORKERS": 5,
    "SAVE_RESULTS": True,
    "PIVOT": True,
    "OUTPUT_BASE": _DEFAULT_OUTPUT,
    "VEGA_PER_TRADE": 100,
}

_CONFIG_PATH = os.getenv(
    "EARNING_TRADE_CONFIG",
    str(Path(__file__).parent / "config.json"),
)

_config_data = {}
try:
    cfg_path = Path(_CONFIG_PATH)
    if cfg_path.exists():
        with open(cfg_path, "r") as f:
            _config_data = json.load(f)
        print(f"✅ Loaded config from {cfg_path}")
    else:
        print(f"⚠️ Config file not found at {cfg_path}, using defaults.")
except Exception as e:
    print(f"⚠️ Failed to read config file {cfg_path}: {e}")
    _config_data = {}


# ---------------------------------------------------------------------
def _get_value(key: str):
    return _config_data.get(key, _DEFAULTS[key])


USE_MULTIPROCESSING: bool = _get_value("USE_MULTIPROCESSING")
MAX_WORKERS: int = _get_value("MAX_WORKERS")
SAVE_RESULTS: bool = _get_value("SAVE_RESULTS")
PIVOT: bool = _get_value("PIVOT")
VEGA_PER_TRADE: int = _get_value("VEGA_PER_TRADE")


def _get_output_base() -> Path:
    base = _config_data.get("OUTPUT_BASE", _DEFAULTS["OUTPUT_BASE"])
    base = os.getenv("EARNING_TRADE_OUTPUT_DIR", base)
    return Path(base)


def _get_output_dir(strategy: str) -> Path:
    """strategy: e.g. 'long' or 'short'"""
    return _get_output_base() / strategy
