import airflow
from airflow import DAG
from airflow.operators import DummyOperator
from airflow.operators import BranchPYthonOperator
from datetime import datetime

project_prefix = airflow.models.Variable.get('project_prefix')
environment = airflow.models.Variable.get('environment')
devops_deploy_github_repo = airflow.models.Variable.get('devops_deploy')


args = {
    'owner': 'Airflow',
    'depends_on_past' : False,
    'start_date': datetime(2020,1,1)
    'retries':0,
}

dag = DAG (
    dag_id = 'im_ingress'
    default_args = args,
    schedule_interval = None, 
    catchup = False, 
    start_date = datetime(2020, 1,1)
    )


step1=BashOperator(
    task_id='Prepare_landing',
    bash_command="rm -rf /tmp/ingress",
    dag=dag
)

step2=BashOperator(
    task_id='Clone_repo',
    bash_command="git clone {} /tmp/ingress/".format(devops_deploy_github_repo),
    dag=dag
)

step3=BashOperator(
    task_id='Launch_VM',
    bash_command="/tmp/egress/im_scripts_UAE/create_egress_instance.sh {0} {1}".format(project_prefix, environment),
    dag=dag
)

step1 >> step2 >> step3
