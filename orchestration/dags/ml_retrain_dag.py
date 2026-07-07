from __future__ import annotations

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}


def _load_training_frame():
    from deltalake import DeltaTable
    from storage.delta_config import SILVER_RIDES_TABLE

    frame = DeltaTable(SILVER_RIDES_TABLE).to_pandas()
    print(f"✓ Loaded {len(frame):,} Silver rows from {SILVER_RIDES_TABLE}")
    return frame


def _extract_ml_features(**context):
    df = _load_training_frame()
    print(f"✓ Extracted {len(df):,} ride records for ML training")
    return df


def _train_surge_model_task(**context):
    try:
        from ml.train_surge_model import train_surge_model

        os.environ.setdefault("MLFLOW_TRACKING_URI", "http://mlflow:5000")

        df = _load_training_frame()

        if len(df) == 0:
            raise ValueError("No data available for ML training!")

        model, metrics = train_surge_model(df, experiment_name="surge_price_prediction")

        mae = metrics.get("mae")
        rmse = metrics.get("rmse")
        mae_str = f"{mae:.4f}" if isinstance(mae, float) else str(mae)
        rmse_str = f"{rmse:.4f}" if isinstance(rmse, float) else str(rmse)
        print(f"✓ Model trained. MAE={mae_str}, RMSE={rmse_str}")
        context["task_instance"].xcom_push(key="model_metrics", value=metrics)
    except Exception as exc:
        print(f"✗ Model training failed: {exc}")
        raise


def _load_nyc_data(**context):
    from ml.nyc_taxi_loader import run_batch_load
    from storage.delta_config import SILVER_RIDES_HISTORICAL_NYC_TABLE

    delta_log_path = os.path.join(SILVER_RIDES_HISTORICAL_NYC_TABLE, "_delta_log")
    if os.path.exists(delta_log_path):
        from deltalake import DeltaTable

        existing_count = len(DeltaTable(SILVER_RIDES_HISTORICAL_NYC_TABLE).to_pandas())
        if existing_count > 0:
            print(f"✓ NYC historical Silver table already has {existing_count:,} rows — skipping download/load")
            return

    run_batch_load()


def _train_nyc_model_task(**context):
    try:
        from ml.nyc_taxi_loader import load_to_silver
        from ml.train_surge_model_nyc import train_surge_model_nyc

        os.environ.setdefault("MLFLOW_TRACKING_URI", "http://mlflow:5000")

        df = load_to_silver()
        model, metrics = train_surge_model_nyc(df)

        mae = metrics.get("mae")
        rmse = metrics.get("rmse")
        mae_str = f"{mae:.4f}" if isinstance(mae, float) else str(mae)
        rmse_str = f"{rmse:.4f}" if isinstance(rmse, float) else str(rmse)
        print(f"✓ NYC model trained. MAE={mae_str}, RMSE={rmse_str}")
        context["task_instance"].xcom_push(key="nyc_model_metrics", value=metrics)
    except Exception as exc:
        print(f"✗ NYC model training failed: {exc}")
        raise


def _promote_if_better(**context):
    try:
        import mlflow
        from mlflow.tracking import MlflowClient

        os.environ.setdefault("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])

        synthetic_metrics = context["task_instance"].xcom_pull(task_ids="train_surge_model", key="model_metrics") or {}
        nyc_metrics = context["task_instance"].xcom_pull(task_ids="train_nyc_model", key="nyc_model_metrics") or {}

        synthetic_mae = synthetic_metrics.get("mae", float("inf"))
        nyc_mae = nyc_metrics.get("mae", float("inf"))

        if synthetic_mae == float("inf") and nyc_mae == float("inf"):
            raise ValueError("No model metrics found for promotion check")

        # Promote whichever candidate has the lower MAE.
        if nyc_mae <= synthetic_mae:
            model_name, mae = "surge_price_model_nyc", nyc_mae
        else:
            model_name, mae = "surge_price_model", synthetic_mae

        promotion_threshold = 15.0

        if mae < promotion_threshold:
            client = MlflowClient()
            versions = client.get_latest_versions(model_name, stages=["None", "Staging"])
            if versions:
                version = versions[-1].version
                client.transition_model_version_stage(
                    name=model_name,
                    version=version,
                    to_stage="Production",
                    archive_existing_versions=True,
                )
                print(f"✓ {model_name} v{version} promoted to Production (MAE={mae:.2f})")
            else:
                print(f"⚠ No model version found to promote for {model_name} — ensure training registered the model")
        else:
            print(f"✗ Model NOT promoted. Best MAE={mae:.2f} >= {promotion_threshold} (baseline)")
    except Exception as exc:
        print(f"✗ Promotion logic failed: {exc}")


with DAG(
    dag_id="ml_retrain_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["ml", "retrain", "daily"],
    default_args=default_args,
) as dag:
    start = EmptyOperator(task_id="start")
    extract = PythonOperator(task_id="extract_ml_features", python_callable=_extract_ml_features)
    train = PythonOperator(task_id="train_surge_model", python_callable=_train_surge_model_task)

    load_nyc_data = PythonOperator(task_id="load_nyc_data", python_callable=_load_nyc_data)
    train_nyc_model = PythonOperator(task_id="train_nyc_model", python_callable=_train_nyc_model_task)

    promote = PythonOperator(task_id="promote_if_better", python_callable=_promote_if_better)
    end = EmptyOperator(task_id="end")

    start >> extract >> train >> promote
    start >> load_nyc_data >> train_nyc_model >> promote
    promote >> end
