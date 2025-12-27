from __future__ import annotations

from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor, as_completed

from tqdm.auto import tqdm

from earning_trade._config import (
    MAX_WORKERS,
    PIVOT,
    SAVE_RESULTS,
    USE_MULTIPROCESSING,
    _get_output_dir,
)
from earning_trade._logging import (
    get_logger,
)
from earning_trade._utils import (
    _get_universe,
)
from earning_trade.strategy_data.long_strategy import (
    EarningsTradeLong,
)
from earning_trade.strategy_data.short_strategy import (
    EarningsTradeShort,
)

logger = get_logger("runner")


def _safe_len(df):
    try:
        return len(df) if df is not None else None
    except Exception:
        return None


def _run_one(ticker: str, *, save: bool, pivot: bool):
    logger.info(f"Starting {ticker}")

    long_out = _get_output_dir("long")
    short_out = _get_output_dir("short")

    long_df = EarningsTradeLong(ticker).run(output_dir=long_out, save=save, pivot=pivot)
    short_df = EarningsTradeShort(ticker).run(output_dir=short_out, save=save, pivot=pivot)

    long_n = _safe_len(long_df)
    short_n = _safe_len(short_df)

    def _status(n):
        return "skipped" if n is None else str(n)

    logger.info(f"{ticker}: done long({_status(long_n)}), short({_status(short_n)})")

    return ticker, (long_n or 0, short_n or 0)


def _iter_universe() -> Iterable[str]:
    return _get_universe().collect()["okey_tk"].to_list()


def main():
    universe = list(_iter_universe())

    if USE_MULTIPROCESSING and len(universe) > 1:
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(_run_one, tk, save=SAVE_RESULTS, pivot=PIVOT): tk for tk in universe}
            for _ in tqdm(as_completed(futs), total=len(futs), desc="Running strategies"):
                pass
    else:
        for tk in tqdm(universe, desc="Running strategies"):
            _run_one(tk, save=SAVE_RESULTS, pivot=PIVOT)


if __name__ == "__main__":
    import multiprocessing as mp
    import sys

    if not hasattr(sys.modules["__main__"], "__spec__"):
        sys.modules["__main__"].__spec__ = None
    mp.set_start_method("spawn", force=True)
    mp.freeze_support()
    main()
