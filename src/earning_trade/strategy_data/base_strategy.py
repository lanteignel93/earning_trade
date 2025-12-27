from __future__ import annotations

from abc import abstractmethod
from pathlib import Path

import polars as pl

from earning_trade._logging import (
    get_logger,
)
from earning_trade._utils import (
    _get_earnings_dates,
)


class EarningsTradeBase:
    CALENDAR_DAYS_FROM_EARNING = 14
    PNL_SIGN = 1
    COUNT_LIMIT = 8

    @property
    def cat(self):
        from earning_trade.mock_catalog import cat

        return cat

    def __init__(self, ticker: str):
        self.logger = get_logger(__name__)
        self.ticker = ticker
        self.skip = False  # determined later

    def _get_option_data(self) -> pl.LazyFrame:
        return (
            self.cat.sr_int_option_close.to_lazy()
            .filter(pl.col("okey_tk") == self.ticker)
            .select(
                [
                    "okey_date",
                    "okey_tk",
                    "okey_xx",
                    "okey_cp",
                    "tradingDate",
                    "srPrc",
                    "srVol",
                    "de",
                    "ve",
                    "uClose",
                ]
            )
        )

    def _calculate_pnl(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(pnl=pl.col("exit_sprc") - pl.col("enter_sprc"))

    def _pivot_data(self, lf: pl.LazyFrame) -> pl.DataFrame:
        df = (
            lf.collect()
            .pivot(
                on="okey_cp",
                index=[
                    "tradingDate",
                    "enterTradeDate",
                    "earnDate",
                    "earnTime",
                    "okey_date",
                    "okey_tk",
                    "okey_xx",
                ],
                values=[
                    "enter_sprc",
                    "enter_iv",
                    "enter_de",
                    "enter_ve",
                    "enter_uprc",
                    "exit_sprc",
                    "exit_iv",
                    "exit_uprc",
                    "pnl",
                ],
            )
            .sort("tradingDate")
            .with_columns(straddle_pnl=self.PNL_SIGN * (pl.col("pnl_Call") + pl.col("pnl_Put")))
            .with_columns(straddle_ve=(pl.col("enter_ve_Call") + pl.col("enter_ve_Put")))
        )
        return df

    def _save_result(self, df: pl.DataFrame, output_dir: str):
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        out_path = Path(output_dir) / f"{self.ticker}.parquet"
        df.write_parquet(out_path)

    @abstractmethod
    def _get_enter_position(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _get_exit_position(self, *args, **kwargs):
        raise NotImplementedError

    def run(
        self,
        output_dir: Path | str | None = None,
        *,
        save: bool = True,
        pivot: bool = True,
    ):
        strat = type(self).__name__
        try:
            self.earn_dates = _get_earnings_dates(self.ticker)
            count = self.earn_dates.collect().height
            if count < self.COUNT_LIMIT:
                self.logger.info(f"{self.ticker} [{strat}]: skipping (only {count} earnings data).")
                return None
        except Exception as e:
            self.logger.exception(f"{self.ticker} [{strat}]: error fetching earnings dates ({e})")
            return None

        try:
            enter_lf = self._get_enter_position(self.earn_dates, self.ticker)
            exit_lf = self._get_exit_position(enter_lf, self.ticker)
            pnl_lf = self._calculate_pnl(exit_lf)
            df = self._pivot_data(pnl_lf) if pivot else pnl_lf.collect()

            if save and output_dir is not None:
                self._save_result(df, output_dir)

            self.logger.info(f"Completed {self.ticker} [{strat}] ({len(df)} rows). Saved={save}")
            return df
        except Exception as e:
            self.logger.exception(f"{self.ticker} [{strat}]: run failed ({e})")
            return None
