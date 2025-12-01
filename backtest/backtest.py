from __future__ import annotations
import polars as pl
from pathlib import Path
import datetime
from onepipeline.laurent_playground.tradedesk_research.earning_trade._config import (
    _get_output_base,
)
from onepipeline.laurent_playground.tradedesk_research.earning_trade._logging import (
    get_logger,
)


class BacktestAggregator:
    def __init__(self, base_dir: Path | str | None = None):
        self.base_dir = Path(base_dir or _get_output_base())
        self.logger = get_logger("aggregator")

    def _load_results(self, strategy: str) -> pl.LazyFrame | None:
        folder = self.base_dir / strategy
        files = list(folder.glob("*.parquet"))
        if not files:
            self.logger.warning(f"No {strategy} parquet files found under {folder}")
            return None
        self.logger.info(f"Scanning {len(files)} {strategy} parquet files ...")
        return pl.scan_parquet([str(f) for f in files]).with_columns(
            pl.lit(strategy.capitalize()).alias("pos_sign")
        )

    def merge_results(self, include: str = "both") -> pl.DataFrame:
        frames = []
        if include in ("long", "both"):
            lf = self._load_results("long")
            if lf is not None:
                frames.append(lf)
        if include in ("short", "both"):
            lf = self._load_results("short")
            if lf is not None:
                frames.append(lf)
        if not frames:
            self.logger.warning("No frames to merge.")
            return pl.DataFrame()
        df = pl.concat([f.collect() for f in frames])
        self.logger.info(f"Merged {df.height:,} total rows from {len(frames)} sources.")
        return df

    def filter_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.filter(
            (pl.col("enter_sprc_Call") > 0)
            & (pl.col("enter_sprc_Call") < 10)
            & (pl.col("enter_sprc_Put") > 0)
            & (pl.col("enter_sprc_Put") < 10)
            & ((pl.col("enter_de_Call") + pl.col("enter_de_Put")).abs() < 0.2)
            & (pl.col("enter_ve_Call") > 1e-3)
            & (pl.col("enter_ve_Call") < 0.2)
            & (pl.col("enter_ve_Put") > 1e-3)
            & (pl.col("enter_ve_Put") < 0.2)
            & (pl.col("enter_iv_Call") > 0.10)
            & (pl.col("enter_iv_Call") < 2)
            & (pl.col("enter_iv_Put") > 0.10)
            & (pl.col("enter_iv_Put") < 2)
            & (pl.col("tradingDate") >= datetime.date(2017, 1, 1))
        )

    def aggregate_daily(
        self,
        df: pl.DataFrame,
        pnl_col: str = "straddle_pnl",
        group_cols: list[str] | None = None,
        vega_per_trade: float | None = None,
    ) -> pl.DataFrame:
        """Aggregate PnL per day across all tickers."""
        if df.is_empty():
            self.logger.warning("No data to aggregate.")
            return df
        group_cols = group_cols or ["tradingDate", "pos_sign"]
        # NOTE: I had a bug in my data generation part, keeping lines below until I re-run the data fix.
        df = df.with_columns(
            pl.when(pl.col("pos_sign") == "Short")
            .then(-pl.col("straddle_pnl"))
            .otherwise(pl.col("straddle_pnl"))
            .alias("straddle_pnl")
        )

        if vega_per_trade:
            if "straddle_ve" not in df.columns:
                df = df.with_columns(
                    straddle_ve=(pl.col("enter_ve_Call") + pl.col("enter_ve_Put"))
                )
            df = df.with_columns(
                size=(pl.lit(vega_per_trade) / pl.col("straddle_ve"))
            ).with_columns(straddle_pnl_ve=pl.col("size") * pl.col(pnl_col))
            pnl_col = "straddle_pnl_ve"
            df = df.filter(pl.col("straddle_pnl_ve").is_finite())

        df = self.filter_dataframe(df)
        agg_df = (
            df.group_by(group_cols)
            .agg(pl.col(pnl_col).sum().alias("daily_pnl"))
            .sort("tradingDate")
        )
        self.logger.info(
            f"Aggregated {df.height:,} rows → {agg_df.height:,} daily records."
        )
        return agg_df


class Backtest:
    """Coordinates aggregation and produces daily PnL time series."""

    def __init__(
        self,
        include: str = "both",
        vega_per_trade: float | None = None,
        save: bool = True,
    ):
        self.include = include
        self.vega_per_trade = vega_per_trade
        self.save = save
        self.logger = get_logger("backtest")
        self.aggregator = BacktestAggregator()

    def run(self) -> pl.DataFrame:
        df_all = self.aggregator.merge_results(self.include)
        if df_all.is_empty():
            self.logger.warning("No data merged for backtest run.")
            return pl.DataFrame()

        df_daily = self.aggregator.aggregate_daily(
            df_all, vega_per_trade=self.vega_per_trade
        )

        if self.save:
            out = self.aggregator.base_dir / f"backtest_daily_{self.include}.parquet"
            df_daily.write_parquet(out)
            self.logger.info(f"Saved aggregated daily PnL → {out}")

        return df_daily


class BacktestAnalysis:
    """Performs PnL statistics and equity curve generation."""

    def __init__(self, df: pl.DataFrame):
        if df.is_empty():
            raise ValueError("Empty DataFrame passed to BacktestAnalysis.")
        self.df = df
        self.logger = get_logger("analysis")

    def calculate_pnl_statistics(
        self,
        pnl_col_name: str = "daily_pnl",
        pos_sign_col_name: str = "pos_sign",
        annualization_factor: int = 252,
        risk_free_rate_daily: float = 0.0,
        portfolio_base: float = 2_000_000.0,
    ) -> pl.DataFrame:
        df = self.df
        if pnl_col_name not in df.columns:
            raise ValueError(f"PnL column '{pnl_col_name}' not found.")
        pnl_series = (
            df.get_column(pnl_col_name).drop_nulls().filter(df[pnl_col_name] != 0.0)
        )
        if pnl_series.len() < 2:
            return pl.DataFrame({"Statistic": ["Not enough data"], "Value": [None]})
        stats = {}

        # --- Basic stats ---
        stats["Mean Daily PnL"] = pnl_series.mean()
        stats["Std Dev Daily PnL"] = pnl_series.std()

        r = pnl_series / portfolio_base
        r_std = r.std()
        if r_std and r_std > 0:
            stats["Annualized Sharpe Ratio"] = (
                (r.mean() - risk_free_rate_daily) / r_std
            ) * (annualization_factor**0.5)
        else:
            stats["Annualized Sharpe Ratio"] = 0.0

        stats["Total Return (%)"] = r.sum() * 100
        n = pnl_series.len()
        total_return = r.sum()
        if n > 0:
            annualized_return = (1 + total_return) ** (annualization_factor / n) - 1
            stats["Annualized Total Return (%)"] = annualized_return * 100
        else:
            stats["Annualized Total Return (%)"] = 0.0
        stats["Annualized Return Std Dev (%)"] = (
            r_std * (annualization_factor**0.5) * 100
        )

        stats["Win Rate (%)"] = (pnl_series > 0).mean() * 100
        stats["Average Win Amount"] = pnl_series.filter(pnl_series > 0).mean()
        stats["Average Loss Amount"] = pnl_series.filter(pnl_series < 0).mean()

        stats["Max Daily Win"] = pnl_series.max()
        stats["Max Daily Loss"] = pnl_series.min()

        # --- Position proportions ---
        if pos_sign_col_name in df.columns:
            signals = df.filter(pl.col(pnl_col_name) != 0)[
                pos_sign_col_name
            ].drop_nulls()
            n_trades = signals.len()
            if n_trades > 0:
                long_trades = signals.filter(signals == "Long").len()
                stats["Pct Time Long"] = (long_trades / n_trades) * 100
                stats["Pct Time Short"] = 100 - stats["Pct Time Long"]
        else:
            stats["Pct Time Long"] = None
            stats["Pct Time Short"] = None

        stats["Skewness"] = pnl_series.skew()
        stats["Kurtosis"] = pnl_series.kurtosis()

        # --- Drawdown windows ---
        if n >= 5:
            stats["Worst 5d Cum PnL (%)"] = (
                pnl_series.rolling_sum(5).min() / portfolio_base * 100
            )
        if n >= 20:
            stats["Worst 20d Cum PnL (%)"] = (
                pnl_series.rolling_sum(20).min() / portfolio_base * 100
            )

        return pl.DataFrame(
            {"Statistic": list(stats.keys()), "Value": list(stats.values())}
        )

    def equity_curve(
        self, pnl_col: str = "daily_pnl", base: float = 1_000_000.0
    ) -> pl.DataFrame:
        df = self.df.sort("tradingDate").with_columns(
            equity=(pl.col(pnl_col).cum_sum() + base).alias("equity")
        )
        return df
