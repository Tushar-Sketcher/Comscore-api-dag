"""
### Dag Documentation
Reports extracted from comscore api
Owner: [Compintel]\
(https://zodiac.zillowgroup.net/team-detail/compintel)
Wiki: [Comscore]\
(https://zwiki.zillowgroup.net/display/AT/Data+Lake%3A+comScore)
Repo: [Gitlab]\
(https://gitlab.zgtools.net/analytics/airflow/dags/big-data/comscore_monthy)

"""

# System package import
import yaml
from datetime import timedelta
from dateutil.parser import parse

# Airflow package import
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.zacs_plugin import ZacsSparkSubmitOperator
from pathlib import Path

environment = Variable.get('env', default_var='stage')

# Load global configurations
global_config = yaml.safe_load(open(Path(__file__).absolute().parent.joinpath('global_config.yaml')))
config = {**global_config['common'], **global_config[environment]}

version = Path(__file__).absolute().parent.joinpath('VERSION').read_text()
artifactory_repo_path = config['artifactory_path'].format(version=version)
zodiac_config = config['zodiac_config']

reports = config['reports'].split(',')
spark_report_file_path = '{artifactory_repo_path}/scripts'.format(artifactory_repo_path=artifactory_repo_path)
file_path = '{artifactory_repo_path}/dags/report_config'.format(artifactory_repo_path=artifactory_repo_path)

dag_name = 'datalake_comscore_monthly'

# Create the dag
default_args = {
    'owner': 'big-data-engineering',
    'depends_on_past': False,
    'start_date': parse(config['start_date']),
    'email': config['email'].split(','),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG(
    dag_name,
    default_args=default_args,
    schedule_interval=config['schedule_interval'],
    max_active_runs=2
)
dag.doc_md = __doc__

# Create tasks
start = DummyOperator(
    task_id='start',
    dag=dag
)

for report in reports:
    report_py = '{}.py'.format(report)
    report_yaml = '{}.yaml'.format(report)
    report_file_path = '{}/{}'.format(file_path, report_yaml)

    if report in config:
        retries = config[report]['retries']
        retry_delay = config[report]['retry_delay']
    else:
        retries = 10  # 10 * 720 (min) = 5 days
        retry_delay = 720  # minutes=720 = 12 hours

    comscore_to_s3_task = ZacsSparkSubmitOperator(
        task_id='import_{report}'.format(report=report),
        zodiac_environment=environment,
        retries=retries,
        retry_delay=timedelta(minutes=retry_delay),
        spark_file=f'{spark_report_file_path}/{report_py}',
        conf={'spark.rpc.message.maxSize': 256},
        zodiac_info=zodiac_config,
        image=config['spark_docker_image'],
        storage_role_arn=config['role_arn'],
        driver_memory='10G',
        files=f'{report_file_path}',
        dag=dag,
        app_arguments=[
            "--role-arn", "{role_arn}".format(role_arn=config['role_arn']),
            "--month", "{month}".format(month='{{ ds }}'),
            "--table", "{table}".format(table=report),
            "--output-path", "{bucket_name}".format(bucket_name=config['bucket_name']),
            "--api-call", "{api_call}".format(api_call=config['api_call']),
            "--start-date", "{start_date}".format(start_date=config['start_date']),
            "--start-month-id", "{start_month_id}".format(start_month_id=config['start_month_id']),
            "--secrets-role", "{secrets_role}".format(secrets_role=config['secrets_role'])
        ]
    )

    # Set up task dependencies
    start >> comscore_to_s3_task
