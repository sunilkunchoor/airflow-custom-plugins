from __future__ import annotations
import os
import json
from typing import Optional

from airflow.models.dag import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksNotebookOperator,
)
from airflow.providers.databricks.operators.databricks_workflow import DatabricksWorkflowTaskGroup
from airflow.utils.timezone import datetime

DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "circus_maximus")
DATABRICKS_NOTIFICATION_EMAIL = os.getenv("DATABRICKS_NOTIFICATION_EMAIL", "sunilkunchoor@gmail.com")


class CustomDatabricksNotebookOperator(DatabricksNotebookOperator):
    """
    Custom operator that runs a Databricks notebook.
    Supports Databricks 'For Each' tasks via `databricks_loop_inputs`.
    """
    template_fields = DatabricksNotebookOperator.template_fields + ("databricks_loop_inputs", "loop_notebook_params", "databricks_loop_concurrency")
    
    # We must explicitly capture notebook_params to conditionally hide it from the parent class
    def __init__(self, *args, databricks_task_type: str = "notebook_task", databricks_loop_inputs: Optional[list] = None, databricks_loop_concurrency: Optional[int] = None, notebook_params: Optional[dict] = None, **kwargs):
        self.databricks_task_type = databricks_task_type
        if self.databricks_task_type == "for_each":
            # When looping, we store params efficiently to prevent them from being hoisted 
            # by the parent operator or TaskGroup to the outer task scope (where {{input}} is invalid).
            self.loop_notebook_params = notebook_params
            # Pass None to parent so it thinks there are no params to hoist
            super().__init__(*args, notebook_params=None, **kwargs)
        else:
            self.loop_notebook_params = None
            super().__init__(*args, notebook_params=notebook_params, **kwargs)
            
        self.databricks_loop_inputs = databricks_loop_inputs
        self.databricks_loop_concurrency = databricks_loop_concurrency


    def _convert_to_databricks_workflow_task(self, context=None, **kwargs):
        """
        Converts the operator into a Databricks Workflow task specification.
        Required for usage within DatabricksWorkflowTaskGroup.
        """
        task_spec = {
            "notebook_task": {
                "notebook_path": self.notebook_path,
                "source": self.source,
            }
        }
        
        if self.notebook_params:
            task_spec["notebook_task"]["base_parameters"] = self.notebook_params
        elif self.loop_notebook_params:
             # Use our isolated params for the inner task logic
            task_spec["notebook_task"]["base_parameters"] = self.loop_notebook_params
            
        if self.notebook_packages:
             # Add logic for packages if needed, simplifying for now as per previous plan
             pass

        # Handle cluster config (simplified matching what triggers would do)
        if self.new_cluster:
            task_spec["new_cluster"] = self.new_cluster
        elif self.existing_cluster_id:
            task_spec["existing_cluster_id"] = self.existing_cluster_id
        elif self.job_cluster_key:
            task_spec["job_cluster_key"] = self.job_cluster_key

        # Databricks task key only allows alphanumeric, - and _ characters.
        # Airflow task_ids inside groups have dots (e.g. group.task).
        # Airflow task_ids inside groups have dots (e.g. group.task).
        # We also need to prepend dag_id to match DatabricksWorkflowTaskGroup expectations
        safe_task_key = f"{self.dag_id}__{self.task_id}".replace(".", "__")

        if self.databricks_task_type == "for_each":
            # For the nested task, we stripped out cluster configuration to ensure
            # it inherits the job cluster and avoids context conflicts.
            
            # Aligning with sample_job.json: Inner task needs its own key
            inner_task_key = f"{safe_task_key}_iteration"
            
            inner_task_spec = {
                "task_key": inner_task_key,
                "notebook_task": task_spec["notebook_task"]
            }
            # Add other safe fields if necessary (e.g. libraries)
            if "libraries" in task_spec:
                inner_task_spec["libraries"] = task_spec["libraries"]
            
            payload = {
                "task_key": safe_task_key,
                "for_each_task": {
                    "inputs": json.dumps(self.databricks_loop_inputs),
                    "task": inner_task_spec
                }
            }
            if self.databricks_loop_concurrency:
                payload["for_each_task"]["concurrency"] = self.databricks_loop_concurrency

            print(f"DEBUG PAYLOAD: {json.dumps(payload)}")
            return payload
        
        # Fallback to standard task structure if no loop inputs
        return {
            "task_key": safe_task_key,
            **task_spec
        }

    def execute(self, context: Context):
        if self.databricks_task_type == "for_each":
            self.log.info("Executing with Databricks For Each Task")
            hook = self._get_hook(caller="execute")
            
            # When running as standalone task (not in workflow group), we might need
            # to wrap it in a job submit call.
            # _convert_to_databricks_workflow_task is for the Group.
            # For execute(), we reuse the payload logic.
            
            payload = self._build_loop_payload()
            self.log.info(f"Payload: {payload}")
            
            # Using official DatabricksHook submit_run
            response = hook.submit_run(payload)
            
            if isinstance(response, (str, int)):
                self.databricks_run_id = response
            elif isinstance(response, dict):
                self.databricks_run_id = response.get("run_id")
            
            if self.wait_for_termination:
                self.monitor_databricks_job()
                
        else:
            super().execute(context)

    def _build_loop_payload(self):
        """Builds the payload for a For Each task run (standalone execution)."""
        # Reuse conversion logic but wrap in job payload
        task_structure = self._convert_to_databricks_workflow_task()
        
        # submit_run expects "tasks": [...]
        payload = {
            "run_name": self.task_id,
            "tasks": [task_structure]
        }
        return payload


dag = DAG(
    dag_id="databricks_dynamic_tasks_2",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "databricks"],
)

with dag:
    workflow = DatabricksWorkflowTaskGroup(
        group_id="workflow",
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_params={"ts": "{{ ts }}"},
        extra_job_params={
            "email_notifications": {
                "on_start": [DATABRICKS_NOTIFICATION_EMAIL],
            },
        },
    )

    with workflow:
        # Using the requested expand method which returns a single task instance
        notebook_1 = CustomDatabricksNotebookOperator(
            task_id="notebook_1",
            databricks_conn_id=DATABRICKS_CONN_ID,
            databricks_task_type="for_each",
            databricks_loop_inputs=["1", "2", "3"],
            databricks_loop_concurrency=3,
            notebook_path="/Users/sunilkunchoor@gmail.com/simple_notebook",
            source="WORKSPACE",
        )