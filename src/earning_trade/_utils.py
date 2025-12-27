import polars as pl

from earning_trade.mock_catalog import cat

SECTOR_INDXES = [
    "XLB",
    "XLC",
    "XLE",
    "XLF",
    "XLI",
    "XLK",
    "XLP",
    "XLRE",
    "XLU",
    "XLV",
    "XLY",
    "XOM",
    "IWM",
    "RSP",
    "QQQ",
    "DOW",
    "SPY",
]


def _get_universe() -> pl.LazyFrame:
    return (
        cat.opt_class.to_lazy()
        .select(["date", "okey_tk", "class_option_volume"])
        .rolling(index_column="date", period="20d", group_by="okey_tk")
        .agg(pl.col("class_option_volume").mean().alias("class_option_volume_rolling"))
        .filter(pl.col("class_option_volume_rolling") > 10000)
        .select("okey_tk")
        .filter(~pl.col("okey_tk").is_in(SECTOR_INDXES))
        .unique()
        .sort("okey_tk")
    )


def _get_earnings_dates(ticker: str) -> pl.LazyFrame:
    return (
        cat.sr_int_ref_earncal()
        .to_lazy()
        .filter(pl.col("ticker_tk") == ticker)
        .filter(pl.col("earnTime").is_in(["AMC", "BMO"]))
        .select(["ticker_tk", "earnDate", "earnTime"])
        .with_columns(pl.col("earnDate").str.to_date())
        .with_columns(tradingDate=pl.col("earnDate"))
        .sort("tradingDate")
        .drop("ticker_tk")
    )
