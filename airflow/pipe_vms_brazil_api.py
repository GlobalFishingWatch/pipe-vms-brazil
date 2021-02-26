from airflow import DAG

from airflow_ext.gfw.models import DagFactory

from datetime import datetime, timedelta

BRAZIL_VMS_SCRAPPER_POOL = 'brazil-vms-scrappers'
PIPELINE = 'pipe_vms_brazil_api'


class PipeVMSBrazilDagFactory(DagFactory):
    """Concrete class to handle the DAG for pipe_vms_brazil_api."""

    def __init__(self, pipeline=PIPELINE, **kwargs):
        """
        Constructs the DAG.

        :@param pipeline: The pipeline name. Default value the PIPELINE.
        :@type pipeline: str.
        :@param kwargs: A dict of optional parameters.
        :@param kwargs: dict.
        """
        super(PipeVMSBrazilDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def source_date(self):
        """
        Validates that the schedule interval only be in daily mode.

        :raise: A ValueError.
        """
        if self.schedule_interval != '@daily':
            raise ValueError('Unsupported schedule interval {}'.format(self.schedule_interval))

    def build(self, dag_id):
        """
        Override of build method.

        :@param dag_id: The id of the DAG.
        :@type table: str.
        """
        config = self.config
        brazil_vms_gcs_path=config['brazil_vms_gcs_path']
        config['brazil_vms_gcs_path']=brazil_vms_gcs_path[:-1] if brazil_vms_gcs_path.endswith('/') else brazil_vms_gcs_path

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:

            prepares_data = self.build_docker_task({
                'task_id':'pipe_brazil_prepares',
                'pool':'k8operators_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'pipe-brazil-prepares',
                'dag':dag,
                'retries':5,
                'max_retry_delay': timedelta(hours=5),
                'arguments':['prepares_brazil_vms_data',
                             '-d {ds}'.format(**config),
                             '-i {brazil_vms_gcs_path}/'.format(**config),
                             '-o {brazil_vms_merged_gcs_path}/'.format(**config)]
            })

            load = self.build_docker_task({
                'task_id':'pipe_brazil_load',
                'pool':'k8operators_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'pipe-brazil-load',
                'dag':dag,
                'retries':5,
                'max_retry_delay': timedelta(hours=5),
                'arguments':['load_brazil_vms_data',
                             '{ds}'.format(**config),
                             '{brazil_vms_merged_gcs_path}'.format(**config),
                             '{project_id}:{brazil_vms_bq_dataset_table}'.format(**config)]
            })

            if (config.get('is_fetch_enabled', False)):
                fetch = self.build_docker_task({
                    'task_id':'pipe_brazil_fetch',
                    'pool':BRAZIL_VMS_SCRAPPER_POOL,
                    'docker_run':'{docker_run}'.format(**config),
                    'image':'{docker_image}'.format(**config),
                    'name':'pipe-brazil-fetch',
                    'dag':dag,
                    'retries':5,
                    'max_retry_delay': timedelta(hours=5),
                    'arguments':['fetch_brazil_vms_data',
                                 '-d {ds}'.format(**config),
                                 '-o {brazil_vms_gcs_path}/'.format(**config),
                                 '-rtr {}'.format(config.get('brazil_api_max_retries', 3))]
                })

                dag >> fetch >> prepares_data >> load
            else:
                for devices_existence in self.source_gcs_sensors(dag, date='devices/{ds}.json.gz'.format(**config)):
                    dag >> devices_existence >> prepares_data

                for messages_existence in self.source_gcs_sensors(dag, date='messages/{ds}.json.gz'.format(**config)):
                    dag >> messages_existence >> prepares_data

                prepares_data >> load

        return dag

pipe_vms_brazil_api_dag = PipeVMSBrazilDagFactory().build(PIPELINE)
