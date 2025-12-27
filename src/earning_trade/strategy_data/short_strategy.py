from __future__ import annotations

import polars as pl

from earning_trade.strategy_data.base_strategy import (
    EarningsTradeBase,
)


class EarningsTradeShort(EarningsTradeBase):
    PNL_SIGN = -1

    def _get_enter_position(self, earn_dates: pl.LazyFrame, ticker: str) -> pl.LazyFrame:
        opt_data = self._get_option_data()

        opt_data = (
            opt_data.with_columns(dist_to_strike=(pl.col("okey_xx") - pl.col("uClose")).abs())
            .sort(["tradingDate", "okey_cp", "okey_date", "dist_to_strike"])
            .group_by(["tradingDate", "okey_cp", "okey_date"], maintain_order=True)
            .first()
        )

        # get prev and next trading days
        dates = (
            opt_data.select("tradingDate")
            .unique()
            .with_columns(prevtradingDate=pl.col("tradingDate").shift(1))
            .with_columns(nexttradingDate=pl.col("tradingDate").shift(-1))
        )

        earn_dates = earn_dates.join(dates, left_on="earnDate", right_on="tradingDate")
        lf = opt_data.join_asof(earn_dates, on="tradingDate", strategy="forward")

        lf = (
            lf.with_columns(
                pl.when(pl.col("earnTime") == "AMC")
                .then(pl.col("earnDate"))
                .otherwise(pl.col("prevtradingDate"))
                .alias("enterTradeDate")
            )
            .with_columns(
                pl.when(pl.col("earnTime") == "AMC")
                .then(pl.col("nexttradingDate"))
                .otherwise(pl.col("earnDate"))
                .alias("exitTradeDate")
            )
            .filter(pl.col("tradingDate") == pl.col("enterTradeDate"))
            .with_columns(
                dist_exp_post_earn=(pl.col("okey_date") - pl.col("enterTradeDate")).dt.total_days()
            )
            .filter(pl.col("dist_exp_post_earn") > 0)
            .sort(["enterTradeDate", "okey_cp", "dist_exp_post_earn"])
            .group_by(["enterTradeDate", "okey_cp"], maintain_order=True)
            .first()
            .rename(
                {
                    "srPrc": "enter_sprc",
                    "de": "enter_de",
                    "ve": "enter_ve",
                    "srVol": "enter_iv",
                    "uClose": "enter_uprc",
                }
            )
            .drop(["prevtradingDate", "nexttradingDate"])
        )

        return lf

    def _get_exit_position(self, enter_lf: pl.LazyFrame, ticker: str) -> pl.LazyFrame:
        opt_data = (
            self.cat.sr_int_option_close.to_lazy()
            .filter(pl.col("okey_tk") == ticker)
            .select(
                [
                    "okey_date",
                    "okey_tk",
                    "okey_xx",
                    "okey_cp",
                    "tradingDate",
                    "srPrc",
                    "srVol",
                    "uClose",
                ]
            )
            .rename({"srPrc": "exit_sprc", "srVol": "exit_iv", "uClose": "exit_uprc"})
        )

        return enter_lf.join(
            opt_data,
            left_on=["okey_date", "okey_tk", "okey_xx", "okey_cp", "exitTradeDate"],
            right_on=["okey_date", "okey_tk", "okey_xx", "okey_cp", "tradingDate"],
            how="left",
        )
