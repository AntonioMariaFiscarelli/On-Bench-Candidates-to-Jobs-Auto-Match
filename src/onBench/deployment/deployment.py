from src.onBench import logger

from pathlib import Path

from src.onBench.utils.snowflake_utils import generate_create_stage_command

from src.onBench.pipeline.stage_01_data_ingestion import run_data_ingestion_pipeline
from src.onBench.pipeline.stage_02_data_transformation import run_data_transformation_pipeline

from src.onBench.entity.config_entity import DeploymentConfig

from src.onBench.config.configuration import ConfigurationManager



class Deployment:
    """
    This class is used to manage the deployment of the pipeline to Snowflake

    This class provides methods to create Snowflake stages, deploy stored procedures, create and delete tasks.

    """
    def __init__(self, config: DeploymentConfig):
        self.config = config

    def create_stage(self, session, is_permanent=True, overwrite=False):
        """Creates Snowflake stage

        Args:
            session (Session): Snowflake session
            is_permanent (string): whether the stage is permanent or temporary
            overwrite (string): whether the stage should be overwritten or not
        Returns:
        """

        stage_sp = self.config.stage_sp

        if overwrite:
            session.sql(f"DROP STAGE IF EXISTS {stage_sp}").collect()
        session.sql(
            generate_create_stage_command(
                stage_sp,
                is_permanent=is_permanent,
                overwrite=True
            )
        ).collect()
        logger.info(f"Stage {stage_sp} successfully created.")


    def upload_config_files(self, session):
        """Upload cpnfig/schema/params files from local directory to the Snowflake stage

        Args:
            session (Session): Snowflake session
        Returns:
        """
        config_dir = self.config.config_dir
        stage_sp = self.config.stage_sp
        local_config_dir = Path(config_dir)

        for file in local_config_dir.glob("*.yaml"):
            session.file.put(
                str(file),
                stage_sp,
                auto_compress=False,
                overwrite=True
            )
            logger.info(f"File {str(file)} uploaded on stage {stage_sp}.")



    def deploy_data_ingestion_stored_procedure(self, session):
        """Deploys the data ingestion stage to Snowflake as a stored procedure

        Args:
            session (Session): Snowflake session
        Returns:
        """
        stage_sp = self.config.stage_sp
        data_ingestion_sp = self.config.data_ingestion_sp
        runtime_version=self.config.runtime_version
        data_ingestion_imports=self.config.data_ingestion_imports
        data_ingestion_packages=self.config.data_ingestion_packages

        session.sql(f"DROP PROCEDURE IF EXISTS {data_ingestion_sp}();").collect()
        session.sproc.register(
            run_data_ingestion_pipeline, 
            name = data_ingestion_sp,
            is_permanent = True,
            stage_location = stage_sp,
            replace = True,
            runtime_version = runtime_version,
            imports =  data_ingestion_imports,
            packages = data_ingestion_packages
        )
        logger.info(f"Stored procedure {data_ingestion_sp} successfully deployed on stage {stage_sp}.")

        

    def deploy_data_transformation_stored_procedure(self, session):
        """Deploys the data tranformation stage to Snowflake as a stored procedure

        Args:
            session (Session): Snowflake session
        Returns:
        """
        stage_sp = self.config.stage_sp
        data_transformation_sp = self.config.data_transformation_sp
        runtime_version=self.config.runtime_version
        data_transformation_imports=self.config.data_transformation_imports
        data_transformation_packages=self.config.data_transformation_packages
        
        session.sql(f"DROP PROCEDURE IF EXISTS {data_transformation_sp}();").collect()
        session.sproc.register(
            run_data_transformation_pipeline, 
            name = data_transformation_sp,
            is_permanent = True,
            stage_location = stage_sp,
            replace = True,
            runtime_version = runtime_version,
            imports =  data_transformation_imports,
            packages = data_transformation_packages
        )
        logger.info(f"Stored procedure {data_transformation_sp} successfully deployed on stage {stage_sp}.")


    def create_tasks(self, session):
        """Creates all tasks:
            - Data ingestion task will execute the data ingestion stored procedure every day at 8AM from Monday to Friday
            - Data transformation task will execute after that the data ingestion task is completed
        Args:
            session (Session): Snowflake session
        Returns:
        """
        database = self.config.database
        schema = self.config.schema
        warehouse = self.config.warehouse
        data_ingestion_sp = self.config.data_ingestion_sp
        data_transformation_sp = self.config.data_transformation_sp
        data_ingestion_task = self.config.data_ingestion_task
        data_transformation_task = self.config.data_transformation_task

        tasks = [data_ingestion_task, data_transformation_task]
        # Suspends existing tasks if they exist already
        for task_name in tasks:
            result = session.sql(f"SHOW TASKS LIKE '{task_name}'").collect()
            session.sql(f"ALTER TASK {task_name} SUSPEND").collect() if result else print(f"Task {task_name} does not exist.")

        # Creates tasks
        data_ingestion_task_sql = f"""
            CREATE OR REPLACE TASK {data_ingestion_task}
            WAREHOUSE = '{warehouse}'
            SCHEDULE = 'USING CRON 0/2 9-19 * * 1-5 CET'
            --SCHEDULE = 'USING CRON 0 8 * * 1-5 CET'
            AS
            CALL {database}.{schema}.{data_ingestion_sp}();
        """
        session.sql(data_ingestion_task_sql).collect()
        logger.info(f"Task {data_ingestion_task} successfully created.")
        
        data_transformation_task_sql = f"""
            CREATE OR REPLACE TASK {data_transformation_task}
            WAREHOUSE = '{warehouse}'
            AFTER {data_ingestion_task}
            AS
            CALL {database}.{schema}.{data_transformation_sp}();
        """
        session.sql(data_transformation_task_sql).collect()
        logger.info(f"Task {data_transformation_task} successfully created.")

        # Resume all tasks. It must be done in reverse order of execution
        for task_name in reversed(tasks):
            session.sql(f"ALTER TASK {task_name} RESUME").collect()
            logger.info(f"Task {task_name} resumed")


    def drop_tasks(self, session):
        """Drops all tasks
        Args:
            session (Session): Snowflake session
        Returns:
        """
        data_ingestion_task = self.config.data_ingestion_task
        data_transformation_task = self.config.data_transformation_task

        tasks = [data_ingestion_task, data_transformation_task]

        # Suspend existing tasks if they exist
        logger.info(f"Deleting tasks...")
        for task_name in tasks:
            result = session.sql(f"SHOW TASKS LIKE '{task_name}'").collect()
            if result:
                session.sql(f"ALTER TASK {task_name} SUSPEND").collect()
                session.sql(f"DROP TASK {task_name}").collect()
                logger.info(f"Task {task_name} successfully deleted.")
            else:
                logger.info(f"Task {task_name} does not exist yet.")

    def reset_pipeline(self, session):
        """Resets workflows, deletes all log tables
        Args:
            session (Session): Snowflake session
        Returns:
        """
        candidate_log_table = self.config.candidate_log_table
        vacancy_log_table = self.config.vacancy_log_table

        session.sql(f"DROP TABLE IF EXISTS {candidate_log_table}").collect()
        session.sql(f"DROP TABLE IF EXISTS {vacancy_log_table}").collect()


def deploy_pipeline(session):
    STAGE_NAME = "Deployment stage"
    try:
        config = ConfigurationManager()
        deployment_config = config.get_deployment_config()
        deployment = Deployment(config=deployment_config)
        deployment.create_stage(session)
        deployment.upload_config_files(session)
        deployment.deploy_data_ingestion_stored_procedure(session)
        deployment.deploy_data_transformation_stored_procedure(session)
        deployment.reset_pipeline(session)
        deployment.drop_tasks(session)
        deployment.create_tasks(session)
        logger.info(f">>>>>> {STAGE_NAME} completed <<<<<<\n\nx==========x")
    except Exception as e:
        logger.exception(e)
        raise e
