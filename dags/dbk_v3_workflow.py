from __future__ import annotations
import os
import datetime
from airflow.sdk import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksNotebookOperator,
    DatabricksTaskBaseOperator
)
from airflow.providers.databricks.operators.databricks_workflow import (
    DatabricksWorkflowTaskGroup
)
from airflow.providers.standard.operators.python import PythonOperator

DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "circus_maximus")
DATABRICKS_NOTIFICATION_EMAIL = os.getenv("DATABRICKS_NOTIFICATION_EMAIL", "sunilkunchoor@gmail.com")
USER = os.environ.get("USER","sunil")
GROUP_ID = "1234"

import copy

def update_dag_run_conf(**context):
    dag_run = context["dag_run"]
    # We can read the dag_run.conf safely
    original_conf = dag_run.conf or {}
    new_conf = copy.deepcopy(original_conf)
    new_conf["updated"] = True
    print(f"New configuration to be used: {new_conf}")
    return new_conf


with DAG(
    dag_id="databricks_workflow",
    tags=["example", "databricks","plugins"],
) as dag:

    python_task = PythonOperator(
        task_id="update_dag_run_conf",
        python_callable=update_dag_run_conf,
    )

    task_group_1 = DatabricksWorkflowTaskGroup(
        group_id=f"test_workflow_{USER}_{GROUP_ID}",
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_params={
            "ts": "{{ ts }}",
        },
        extra_job_params={
            "email_notifications": {
                "on_start": [DATABRICKS_NOTIFICATION_EMAIL],
            },
            "parameters": [
                {
                    "name": "configs",
                    "default": "{{ ti.xcom_pull(task_ids='update_dag_run_conf') }}",
                }
            ],
        },
    )

    with task_group_1:
        notebook_1 = DatabricksNotebookOperator(
            task_id="workflow_notebook_1",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Users/sunilkunchoor@gmail.com/simple_notebook",
            source="WORKSPACE",
        )

        notebook_2 = DatabricksNotebookOperator(
            task_id="workflow_notebook_2",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Users/sunilkunchoor@gmail.com/simple_notebook",
            source="WORKSPACE",
            notebook_params={"foo": "bar", "ds": "{{ ds }}"},
        )

        notebook_3 = DatabricksNotebookOperator(
            task_id="workflow_notebook_3",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Users/sunilkunchoor@gmail.com/simple_notebook",
            source="WORKSPACE",
            notebook_params={"foo": "bar", "ds": "{{ ds }}"},
        )

        notebook_4 = DatabricksNotebookOperator(
            task_id="workflow_notebook_4",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Users/sunilkunchoor@gmail.com/simple_notebook",
            source="WORKSPACE",
            notebook_params={"foo": "bar", "ds": "{{ ds }}"},
        )

        notebook_1 >> notebook_2 >> [notebook_3, notebook_4]

    python_task >> task_group_1
