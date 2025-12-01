# earning_trade/app/aggregate_results.py

from __future__ import annotations
from onepipeline.laurent_playground.tradedesk_research.earning_trade._config import (
    VEGA_PER_TRADE,
)
from onepipeline.laurent_playground.tradedesk_research.earning_trade.backtest.backtest import (
    Backtest,
    BacktestAnalysis,
)
from onepipeline.laurent_playground.tradedesk_research.earning_trade._logging import (
    get_logger,
)


def main(include: str = "both"):
    logger = get_logger("aggregate_app")
    bt = Backtest(include=include, vega_per_trade=VEGA_PER_TRADE, save=True)
    daily_df = bt.run()

    if daily_df.is_empty():
        logger.warning("No daily PnL output; aborting analysis.")
        return

    # Save the daily timeseries explicitly
    out_path = bt.aggregator.base_dir / f"daily_timeseries_{include}.parquet"
    daily_df.write_parquet(out_path)
    logger.info(f"Saved daily time series to {out_path}")

    # Run analysis
    analysis = BacktestAnalysis(daily_df)
    stats = analysis.calculate_pnl_statistics()
    logger.info("PnL Summary:\n" + stats.to_pandas().to_string(index=False))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Aggregate and analyze backtest results"
    )
    parser.add_argument(
        "--include",
        choices=["long", "short", "both"],
        default="both",
        help="Which strategy results to include",
    )
    args = parser.parse_args()
    main(args.include)
