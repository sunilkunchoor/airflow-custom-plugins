from airflow.plugins_manager import AirflowPlugin
from airflow.sdk.bases.operatorlink import BaseOperatorLink
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from airflow.models import XCom
from airflow.utils.log.logging_mixin import LoggingMixin

from typing import Optional, Dict, Any


class DatabricksNotebookLink(BaseOperatorLink, LoggingMixin):
    name = "Databricks Notebook Link"

    operators = [DatabricksNotebookOperator]

    def get_link(self, operator, *, ti_key, **context) -> str:
        try:
            link = XCom.get_value(
                ti_key=ti_key,
                key=self.xcom_key,
            )
            return link if link else ""
        except Exception as e:
            self.log.warning("Failed to retrieve Databricks job run link from XCom: %s", e)
            return ""

class DatabricksCustomPlugin(AirflowPlugin):
    name = "databricks_custom_plugin"

    operator_extra_links = [
        DatabricksNotebookLink(),
    ]