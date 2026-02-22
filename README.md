# MLOps Mini Pipeline

A reproducible, containerised batch pipeline that computes a rolling-mean signal
rate over cryptocurrency OHLCV data.

---

## Project Structure

```
.
├── run.py            # Main pipeline script
├── config.yaml       # Pipeline configuration
├── data.csv          # Input OHLCV dataset (10,000 rows)
├── requirements.txt  # Python dependencies
├── Dockerfile        # Container definition
├── metrics.json      # output (from a successful run)
├── run.log           # log file (from a successful run)
└── README.md         # This file
```

---

## Dependencies

| Package  | Version |
|----------|---------|
| pandas   | 2.0.3   |
| numpy    | 1.24.4  |
| pyyaml   | 6.0.1   |

---

## Setup Instructions

```bash
# Create and activate a virtual environment
python -m venv venv
Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

---

## Local Execution

```bash
python run.py \
  --input    data.csv \
  --config   config.yaml \
  --output   metrics.json \
  --log-file run.log
```

---

## Docker Instructions

```bash
# Build the Docker image
docker build -t mlops-task .

# Run the container (automatically executes the pipeline)
docker run --rm mlops-task
```

---

## Expected Output

`metrics.json` will be written with the following structure:

```json
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.499,
  "latency_ms": 134,
  "seed": 42,
  "status": "success"
}
```

On error, the output will be:

```json
{
  "version": "v1",
  "status": "error",
  "error_message": "Description of what went wrong"
}
```

---

## Configuration (`config.yaml`)

| Field   | Value | Description                        |
|---------|-------|------------------------------------|
| seed    | 42    | NumPy random seed for reproducibility |
| window  | 5     | Rolling mean window size           |
| version | v1    | Pipeline version tag               |

---

## Pipeline Logic

1. **Config loading** – reads `config.yaml`, sets NumPy random seed.
2. **Data ingestion** – loads CSV, validates `close` column presence.
3. **Rolling mean** – computes `close.rolling(window, min_periods=1).mean()`.
4. **Signal generation** – `signal = 1` if `close > rolling_mean`, else `0`.
5. **Metrics** – calculates `signal_rate` (mean of signals) and `latency_ms`.
6. **Output** – writes `metrics.json` and `run.log`.
