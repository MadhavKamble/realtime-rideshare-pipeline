from __future__ import annotations

import os
import sys
import time
import pandas as pd

from simulator.ride_generator import generate_ride_event

try:
    from ml.train_surge_model import train_surge_model
except Exception as exc:
    print("Missing ML training dependencies or module import failed:", exc)
    sys.exit(1)


def build_frame(n: int = 300) -> pd.DataFrame:
    rows = [generate_ride_event() for _ in range(n)]
    return pd.DataFrame(rows)


def main():
    print("Building synthetic dataset...")
    df = build_frame(300)
    print(f"Dataset rows: {len(df)}")
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "./data/mlflow")
    print(f"MLflow tracking URI: {tracking_uri}")

    print("Starting training... this may take a minute")
    start = time.time()
    model, metrics = train_surge_model(df, experiment_name="demo_surge")
    elapsed = time.time() - start
    print(f"Training finished in {elapsed:.1f}s")
    print("Metrics:")
    for k, v in metrics.items():
        print(f" - {k}: {v}")

    print("Model and metrics logged to MLflow.")


if __name__ == "__main__":
    main()
