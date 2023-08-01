"""
### Run a dbt Core project as a task group with Cosmos

Simple DAG showing how to run a dbt project as a task group, using
an Airflow connection and injecting a variable into the dbt project.
"""

from airflow.decorators import dag
from cosmos.providers.dbt.task_group import DbtTaskGroup
from airflow.utils.task_group import TaskGroup
from cosmos.providers.dbt.core.operators import DbtRunOperator, DbtTestOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from pendulum import datetime

CONNECTION_ID = "snowflake_con"
DB_NAME = "TELECOM_ANALYSIS"
SCHEMA_NAME = "DBT"
DBT_PROJECT_NAME = "telecom_analysis_project"
# the path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"
# The path to your dbt root directory
DBT_ROOT_PATH = "/usr/local/airflow/dbt"


@dag(
    start_date=datetime(2023, 6, 1),
    schedule=None,
    catchup=False,
    template_searchpath=["/usr/local/airflow/snowflake_queries"]
)
def my_simple_dbt_dag():


    """
    run_specific_model = DbtRunOperator(
        task_id = 'run_operator',
        conn_id = CONNECTION_ID,
        select = "curated_layer.crm",
        project_dir = "/usr/local/airflow/dbt/telecom_analysis_project",
        dbt_executable_path = DBT_EXECUTABLE_PATH,
        install_deps = True
    )

    test_model = DbtTestOperator(
        task_id = 'test_operator',
        conn_id = CONNECTION_ID,
        select = "crm",
        project_dir = "/usr/local/airflow/dbt/telecom_analysis_project",
        dbt_executable_path = DBT_EXECUTABLE_PATH,
        install_deps = True
    )

    run_specific_model >> test_model
    """

    with TaskGroup(group_id = 'load_s3_data') as tg_load_data:

        copy_into_revenue_table = S3ToSnowflakeOperator(
                task_id="copy_into_revenue_table",
                snowflake_conn_id="s3_to_snowflake_conn",
                s3_keys=["rev1.csv"],
                table="revenue",
                stage="external_stage",
                file_format="(TYPE = csv,FIELD_DELIMITER = ',',SKIP_HEADER = 1, empty_field_as_null = TRUE ,null_if = ('NULL', 'null'), FIELD_OPTIONALLY_ENCLOSED_BY='\"')"
            )
    
        copy_into_crm_table = S3ToSnowflakeOperator(
                task_id="copy_into_crm_table",
                snowflake_conn_id="s3_to_snowflake_conn",
                s3_keys=["crm1.csv"],
                table="crm",
                stage="external_stage",
                file_format="(TYPE = csv,FIELD_DELIMITER = ',',SKIP_HEADER = 1, empty_field_as_null = TRUE ,null_if = ('NULL', 'null'), FIELD_OPTIONALLY_ENCLOSED_BY='\"')"
            )
    
        copy_into_devices_table = S3ToSnowflakeOperator(
            task_id="copy_into_devices_table",
            snowflake_conn_id="s3_to_snowflake_conn",
            s3_keys=["device1.csv"],
            table="devices",
            stage="external_stage",
            file_format="(TYPE = csv,FIELD_DELIMITER = ',',SKIP_HEADER = 1, empty_field_as_null = TRUE ,null_if = ('NULL', 'null'), FIELD_OPTIONALLY_ENCLOSED_BY='\"')"
        )

        [copy_into_revenue_table, copy_into_crm_table, copy_into_devices_table]

    transform_data = DbtTaskGroup(
        group_id="transform_data",
        dbt_project_name=DBT_PROJECT_NAME,
        conn_id=CONNECTION_ID,
        dbt_root_path=DBT_ROOT_PATH,
        dbt_args={
            "dbt_executable_path": DBT_EXECUTABLE_PATH,
            "install_deps": True
        },
    )

    create_output_table = SnowflakeOperator(
        task_id = 'create_output_table',
        sql = "merge.sql",
        snowflake_conn_id = CONNECTION_ID
    )

    unload_output_to_s3 = SnowflakeOperator(
        task_id = 'load_output_table_to_s3',
        sql = "copy_to_s3.sql",
        snowflake_conn_id = CONNECTION_ID
    )

    tg_load_data >> transform_data >> create_output_table >> unload_output_to_s3

my_simple_dbt_dag()
