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


def _promote_if_better(**context):
    try:
        import mlflow
        from mlflow.tracking import MlflowClient

        os.environ.setdefault("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])

        metrics = context["task_instance"].xcom_pull(task_ids="train_surge_model", key="model_metrics")

        if not metrics:
            raise ValueError("No model metrics found for promotion check")

        mae = metrics.get("mae", float("inf"))
        promotion_threshold = 15.0

        if mae < promotion_threshold:
            client = MlflowClient()
            versions = client.get_latest_versions("surge_price_model", stages=["None", "Staging"])
            if versions:
                version = versions[-1].version
                client.transition_model_version_stage(
                    name="surge_price_model",
                    version=version,
                    to_stage="Production",
                    archive_existing_versions=True,
                )
                print(f"✓ surge_price_model v{version} promoted to Production (MAE={mae:.2f})")
            else:
                print("⚠ No model version found to promote — ensure train_surge_model registered the model")
        else:
            print(f"✗ Model NOT promoted. MAE={mae:.2f} >= {promotion_threshold} (baseline)")
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
    promote = PythonOperator(task_id="promote_if_better", python_callable=_promote_if_better)
    end = EmptyOperator(task_id="end")

    start >> extract >> train >> promote >> end
