"""
MLOps Engineering Internship - Technical Assessment
Mini MLOps Pipeline: Rolling Mean Signal Generator
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


def setup_logging(log_file: str) -> logging.Logger:
    """Configure logging to file and stdout."""
    logger = logging.getLogger("mlops_pipeline")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")

    # File handler
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger


def load_config(config_path: str, logger: logging.Logger) -> dict:
    """Load and validate YAML configuration file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, "r") as f:
        config = yaml.safe_load(f)

    required_keys = ["seed", "window", "version"]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required config key: '{key}'")

    logger.info(f"Config loaded: seed={config['seed']}, window={config['window']}, version={config['version']}")
    return config


def load_data(input_path: str, logger: logging.Logger) -> pd.DataFrame:
    """Load and validate the input CSV file."""
    path = Path(input_path)

    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    try:
        df = pd.read_csv(path)
    except Exception as e:
        raise ValueError(f"Invalid CSV file format: {e}")

    if df.empty:
        raise ValueError("Input CSV file is empty.")

    if "close" not in df.columns:
        raise ValueError("Required column 'close' is missing from the dataset.")

    logger.info(f"Data loaded: {len(df)} rows")
    return df


def compute_rolling_mean(df: pd.DataFrame, window: int, logger: logging.Logger) -> pd.DataFrame:
    """Calculate rolling mean on the close column."""
    df = df.copy()
    df["rolling_mean"] = df["close"].rolling(window=window, min_periods=1).mean()
    logger.info(f"Rolling mean calculated with window={window}")
    return df


def generate_signals(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Generate buy/hold signals based on close vs rolling mean."""
    df = df.copy()
    df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)
    logger.info("Signals generated")
    return df


def write_metrics(output_path: str, metrics: dict):
    """Write metrics dictionary to a JSON file."""
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(json.dumps(metrics, indent=2))


def main():
    parser = argparse.ArgumentParser(description="MLOps Mini Pipeline")
    parser.add_argument("--input",    required=True, help="Path to input CSV file")
    parser.add_argument("--config",   required=True, help="Path to YAML config file")
    parser.add_argument("--output",   required=True, help="Path for output metrics JSON")
    parser.add_argument("--log-file", required=True, help="Path for log file")
    args = parser.parse_args()

    # Setup logging first (always needed, even for error output)
    logger = setup_logging(args.log_file)
    logger.info("Job started")

    start_time = time.time()

    try:
        # 1. Load configuration
        config = load_config(args.config, logger)
        seed    = config["seed"]
        window  = config["window"]
        version = config["version"]

        # 2. Set random seed for reproducibility
        np.random.seed(seed)

        # 3. Load data
        df = load_data(args.input, logger)

        # 4. Rolling mean
        df = compute_rolling_mean(df, window, logger)

        # 5. Signal generation
        df = generate_signals(df, logger)

        # 6. Metrics calculation
        rows_processed = len(df)
        signal_rate    = round(float(df["signal"].mean()), 4)
        latency_ms     = int((time.time() - start_time) * 1000)

        logger.info(f"Metrics: signal_rate={signal_rate}, rows_processed={rows_processed}")
        logger.info(f"Job completed successfully in {latency_ms}ms")

        metrics = {
            "version":        version,
            "rows_processed": rows_processed,
            "metric":         "signal_rate",
            "value":          signal_rate,
            "latency_ms":     latency_ms,
            "seed":           seed,
            "status":         "success"
        }
        write_metrics(args.output, metrics)
        sys.exit(0)

    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(f"Pipeline failed: {e}")

        # Try to get version from config if available
        version = "unknown"
        try:
            with open(args.config) as f:
                cfg = yaml.safe_load(f)
                version = cfg.get("version", "unknown")
        except Exception:
            pass

        error_metrics = {
            "version":       version,
            "status":        "error",
            "error_message": str(e)
        }
        write_metrics(args.output, error_metrics)
        sys.exit(1)


if __name__ == "__main__":
    main()
