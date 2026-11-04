from src.onBench import logger

from pathlib import Path

from src.onBench.utils.snowflake_utils import generate_create_stage_command

from src.onBench.pipeline.stage_01_data_ingestion import run_data_ingestion_pipeline
from src.onBench.pipeline.stage_02_data_transformation import run_data_transformation_pipeline

from src.onBench.entity.config_entity import DeploymentConfig, GlobalConfig

from src.onBench.config.configuration import ConfigurationManager



class Deployment:
    """
    This class is used to manage the deployment of the pipeline to Snowflake

    This class provides methods to create Snowflake stages, deploy stored procedures, create and delete tasks.

    """
    def __init__(self, config: DeploymentConfig, global_config: GlobalConfig):
        self.config = config
        self.global_config = global_config

    def create_stage(self, session, is_permanent=True, overwrite=False):
        """Creates Snowflake stage

        Args:
            session (Session): Snowflake session
            is_permanent (string): whether the stage is permanent or temporary
            overwrite (string): whether the stage should be overwritten or not
        Returns:
        """
        config = self.config
        stage = config.stage

        if overwrite:
            session.sql(f"DROP STAGE IF EXISTS {stage}").collect()
        session.sql(
            generate_create_stage_command(
                stage,
                is_permanent=is_permanent,
                overwrite=True
            )
        ).collect()
        logger.info(f"Stage {stage} successfully created.")


    def upload_config_files(self, session):
        """Upload cpnfig/schema/params files from local directory to the Snowflake stage

        Args:
            session (Session): Snowflake session
        Returns:
        """
        config = self.config
        config_dir = config.config_dir
        stage = config.stage
        local_config_dir = Path(config_dir)

        for file in local_config_dir.glob("*.yaml"):
            session.file.put(
                str(file),
                stage,
                auto_compress=False,
                overwrite=True
            )
            logger.info(f"File {str(file)} uploaded on stage {stage}.")



    def deploy_data_ingestion_stored_procedure(self, session):
        """Deploys the data ingestion stage to Snowflake as a stored procedure

        Args:
            session (Session): Snowflake session
        Returns:
        """
        config = self.config
        stage = config.stage
        stored_procedure = config.data_ingestion.stored_procedure
        runtime_version = config.runtime_version
        imports = config.data_ingestion.imports
        packages = config.data_ingestion.packages

        session.sql(f"DROP PROCEDURE IF EXISTS {stored_procedure}();").collect()
        session.sproc.register(
            run_data_ingestion_pipeline, 
            name = stored_procedure,
            is_permanent = True,
            stage_location = stage,
            replace = True,
            runtime_version = runtime_version,
            imports =  imports,
            packages = packages
        )
        logger.info(f"Stored procedure {stored_procedure} successfully deployed on stage {stage}.")

        

    def deploy_data_transformation_stored_procedure(self, session):
        """Deploys the data tranformation stage to Snowflake as a stored procedure

        Args:
            session (Session): Snowflake session
        Returns:
        """
        config = self.config
        stage = config.stage
        stored_procedure = config.data_transformation.stored_procedure
        runtime_version=config.runtime_version
        imports=config.data_transformation.imports
        packages=config.data_transformation.packages
        
        session.sql(f"DROP PROCEDURE IF EXISTS {stored_procedure}();").collect()
        session.sproc.register(
            run_data_transformation_pipeline, 
            name = stored_procedure,
            is_permanent = True,
            stage_location = stage,
            replace = True,
            runtime_version = runtime_version,
            imports =  imports,
            packages = packages
        )
        logger.info(f"Stored procedure {stored_procedure} successfully deployed on stage {stage}.")


    def create_tasks(self, session):
        """Creates all tasks:
            - Data ingestion task will execute the data ingestion stored procedure every day at 8AM from Monday to Friday
            - Data transformation task will execute after that the data ingestion task is completed
        Args:
            session (Session): Snowflake session
        Returns:
        """
        config = self.config
        database = config.database
        schema = config.schema
        warehouse = config.warehouse
        data_ingestion_sp = config.data_ingestion.stored_procedure
        data_transformation_sp = config.data_transformation.stored_procedure
        data_ingestion_task = config.data_ingestion.task
        data_transformation_task = config.data_transformation.task

        tasks = [data_ingestion_task, data_transformation_task] # [data_transformation_task]
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
            --SCHEDULE = 'USING CRON 0/2 9-19 * * 1-5 CET'
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
        config = self.config
        data_ingestion_task = config.data_ingestion.task
        data_transformation_task = config.data_transformation.task

        tasks = [data_ingestion_task, data_transformation_task] #[data_ingestion_task]

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

        global_config = self.global_config
        candidate_log_table = global_config.candidate_log_table
        joborder_log_table = global_config.joborder_log_table

        session.sql(f"DROP TABLE IF EXISTS {candidate_log_table}").collect()
        session.sql(f"DROP TABLE IF EXISTS {joborder_log_table}").collect()


def deploy_pipeline(session):
    STAGE_NAME = "Deployment stage"
    try:
        config = ConfigurationManager()
        deployment_config = config.get_deployment_config()
        global_config = config.get_global_config()
        deployment = Deployment(config=deployment_config, global_config=global_config)
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
