# Earnings Trade (US Equities)

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A systematic long/short trading strategy targeting post-earnings drift and volatility anomalies in US Equities.
# Architecture

This project uses a modern `src` layout for reliability and testing.

```text
earning_trade/
├── src/earning_trade/
│   ├── app/           # Execution/Dashboard logic
│   ├── backtest/      # Historical simulation engine
│   ├── _config.py      # Strategy parameters
│   └── _utils.py       # Data loaders & math helpers
├── notebooks/         # Research & Prototyping
└── tests/             # Unit tests
```

# Getting Started
We use uv for fast dependency management.
```bash
# 1. Install Dependencies
uv sync

# 2. Activate Environment
source .venv/bin/activate

# 3. Run Pre-commit hooks
pre-commit install
```

# Strategy Overview

* Universe: US Large-Cap Equities.

* Signal: Analysis of implied vs. realized volatility leading into earnings announcements.

* Execution: Automated signals generated via `app/`.
