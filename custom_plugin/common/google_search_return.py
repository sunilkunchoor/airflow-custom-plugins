from airflow.plugins_manager import AirflowPlugin
from airflow.sdk.bases.operatorlink import BaseOperatorLink
from airflow.models.xcom import XComModel
from airflow.utils.session import create_session
from airflow.providers.standard.operators.bash import BashOperator
from urllib.parse import quote_plus
from typing import Optional, Dict, Any

from airflow.providers.databricks.operators.databricks import (
    DatabricksNotebookOperator
)

class GoogleSearchXComLink(BaseOperatorLink):
    name = "🔍 Search Return Value in Google"
    
    operators = [BashOperator, DatabricksNotebookOperator]

    def get_link(self, operator, *, ti_key, **context) -> str:
        
        with create_session() as session:
            result = session.execute(
                XComModel.get_many(
                    key="return_value",
                    run_id=ti_key.run_id,
                    dag_ids=ti_key.dag_id,
                    task_ids=ti_key.task_id,
                    map_indexes=ti_key.map_index,
                ).with_only_columns(XComModel.value)
            ).first()

            if result:
                xcom_value = XComModel.deserialize_value(result)
                search_term = str(xcom_value)
                encoded_search = quote_plus(search_term)
                final_search = f"https://www.google.com/search?q={encoded_search}"
        
        return final_search or "https://www.google.com"


class OperatorExtraLinkPlugin(AirflowPlugin):
    name = "google_search_return"
    operator_extra_links = [GoogleSearchXComLink()]
    global_operator_extra_links = [GoogleSearchXComLink()]
