import logging
import logging.handlers
import os
from datetime import datetime
from pathlib import Path


def get_logger(name: str = "earning_trade") -> logging.Logger:
    """
    Create (or retrieve) a logger that writes both to stdout and a rotating file.
    Safe for multiprocessing: each process reuses same configuration.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(logging.DEBUG)

    # Where to write logs
    base_dir = os.getenv("EARNING_TRADE_OUTPUT_DIR", Path().parent.parent.resolve())
    log_dir = Path(base_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"run_{datetime.now():%Y%m%d}.log"

    # --- Console handler ---
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(
        logging.Formatter("[%(asctime)s] [%(process)d] [%(levelname)s] %(message)s", "%H:%M:%S")
    )

    # --- File handler (rotates daily, keeps 10 days) ---
    fh = logging.handlers.TimedRotatingFileHandler(
        log_file, when="midnight", backupCount=10, encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter(
            "[%(asctime)s] [%(process)d] [%(levelname)s] %(name)s: %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    )

    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.propagate = False

    logger.info(f"Logger initialized → {log_file}")
    return logger
