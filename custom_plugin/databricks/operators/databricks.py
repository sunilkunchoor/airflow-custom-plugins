from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.taskinstance import TaskInstanceKey
from airflow.utils.context import Context
from custom_plugin.databricks.hooks.databricks import DatabricksHook
from typing import Any, Optional
import json

class DatabricksJobRunLink(BaseOperatorLink):
    name = "Databricks Job Run"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        return "https://databricks.com/"

class DatabricksRunNowOperator(BaseOperator):
    """
    Runs an existing Databricks job.
    """
    operator_extra_links = (DatabricksJobRunLink(),)
    template_fields = ("job_id",)

    def __init__(
        self,
        *,
        job_id: str,
        databricks_conn_id: str = "databricks_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id
        self.databricks_conn_id = databricks_conn_id


    def execute(self, context: Context) -> Any:
        hook = DatabricksHook(databricks_conn_id=self.databricks_conn_id)
        self.log.info(f"Triggering run for job: {self.job_id}")
        response = hook.run(endpoint="jobs/run-now", json={"job_id": self.job_id})
        return response

from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator

class CustomDatabricksNotebookOperator(DatabricksNotebookOperator):
    """
    Custom operator that runs a Databricks notebook.
    Supports Databricks 'For Each' tasks via `databricks_loop_inputs`.
    """
    template_fields = DatabricksNotebookOperator.template_fields + ("databricks_loop_inputs",)

    def __init__(self, *args, databricks_loop_inputs: Optional[list] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.databricks_loop_inputs = databricks_loop_inputs

    @classmethod
    def expand(cls, **kwargs) -> "CustomDatabricksNotebookOperator":
        """
        Mimics Airflow's Dynamic Task Mapping `expand` method but creates a single task
        configured for Databricks 'For Each' execution.
        """
        return cls(**kwargs)

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
        safe_task_key = self.task_id.replace(".", "__")

        if self.databricks_loop_inputs:
            return {
                "task_key": safe_task_key,
                "for_each_task": {
                    "inputs": json.dumps(self.databricks_loop_inputs),
                    "task": task_spec
                }
            }
        
        # Fallback to standard task structure if no loop inputs
        return {
            "task_key": safe_task_key,
            **task_spec
        }

    def execute(self, context: Context):
        if self.databricks_loop_inputs:
            self.log.info("Executing with Databricks For Each Task")
            hook = self.get_hook()
            
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
