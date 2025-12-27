from datetime import date, timedelta

import numpy as np
import polars as pl


class MockDataset:
    """Helper class to mimic the behavior of a catalog dataset."""

    def __init__(self, df: pl.DataFrame):
        self.df = df

    def to_lazy(self) -> pl.LazyFrame:
        return self.df.lazy()


class MockCatalog:
    def __init__(self):
        self._opt_class_data = self._generate_opt_class_data()
        self._earncal_data = self._generate_earncal_data()

    @property
    def opt_class(self):
        """Mocks cat.opt_class.to_lazy()"""
        return MockDataset(self._opt_class_data)

    def sr_int_ref_earncal(self):
        """Mocks cat.sr_int_ref_earncal().to_lazy()"""
        return MockDataset(self._earncal_data)

    def _generate_opt_class_data(self) -> pl.DataFrame:
        """Generates fake option volume data for universe selection."""
        dates = [date(2023, 1, 1) + timedelta(days=i) for i in range(30)]
        tickers = ["AAPL", "NVDA", "MSFT", "TSLA", "XLK", "SPY"]

        data = []
        for d in dates:
            for t in tickers:
                vol = np.random.randint(5000, 20000)
                data.append({"date": d, "okey_tk": t, "class_option_volume": vol})

        return pl.DataFrame(data)

    def _generate_earncal_data(self) -> pl.DataFrame:
        """Generates fake earnings calendar data."""
        data = [
            {"ticker_tk": "AAPL", "earnDate": "2023-02-02", "earnTime": "AMC"},
            {"ticker_tk": "NVDA", "earnDate": "2023-02-22", "earnTime": "BMO"},
            {"ticker_tk": "MSFT", "earnDate": "2023-01-24", "earnTime": "TAS"},
            {"ticker_tk": "GOOGL", "earnDate": "2023-02-05", "earnTime": "AMC"},
        ]
        return pl.DataFrame(data)


cat = MockCatalog()
