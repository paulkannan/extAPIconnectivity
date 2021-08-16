
import airflow
from airflow import DAG
from airflow.operators import DummyOperator
from airflow.operators import BranchPYthonOperator
from datetime import datetime
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.bash_operator import BashOperator
import os,sys
sys.path.append(os.path.abspath('/opt/airflow/dags/deployment'))
from variable_mine import dataset_name, table_name, bucket_name, bq_to_gcs_path, ct_colums
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
    dag_id = 'im_egress'
    default_args = args,
    schedule_interval = None, 
    catchup = False, 
    start_date = datetime(2020, 1,1)
    )
now = datetime.now()
previous_year = now.year - 1
current_month = now.month + 1
current_year = now.year()
currdate = datetime.now().strftime("%Y%m%d")

run_this_first=DummyOperator(
    task_id='START'
    dag=dag
)

def _choose_path():
    if current_month in (2,3,4):
        return 'build_ct_lastyear'
    return 'SkipLastYear'

build_ct_lastyear = BigQueryOperator (

    task_id='build_ct_lastyear'
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''SELECT {0} FROM {1} '''.format(ct_columns, project_name + '.' + dataset_name + '.'+ table_name+'_'+str(previous_year)),
    destination_dataset_table=project_name + '.'+dataset_name + '.'+table_name+'_'+str(previous_year)+'_temp',
    dag=dag
)

extract_ct_lastyear = BigQueryToCloudStorageOperator(

    task_id='extract_ct_lastyear'
    source_project_dataset_table= project_name + '.'+dataset_name + '.'+table_name+'_'+str(previous_year)+'_temp'
    destination_cloud_storage_uris = [bucket_name + '/' +bq_to_gcs_path + currdate + '/' + table_name+'_'+str(previous_year)+'_'+currdate+'.csv'],
    export_format='CSV',
    compression='None',
    dag=dag
)

SkipLastYear = DummyOperator(

    task_id='SkipLastYear',
    dag=dag
)

makeDecision = BranchPYthonOperator(
    task_id="makeDecision",
    python_callable=_choose_path,
    trigger_rule='all_success',
    dag=dag
)

step1=BashOperator(
    task_id='Prepare_landing',
    bash_command="rm -rf /tmp/egress",
    dag=dag
)

step2=BashOperator(
    task_id='Clone_repo',
    bash_command="git clone {} /tmp/egress/".format(devops_deploy_github_repo),
    dag=dag
)

step3=BashOperator(
    task_id='Launch_VM',
    bash_command="/tmp/egress/im_scripts_UAE/create_egress_instance.sh {0} {1}".format(project_prefix, environment),
    dag=dag
)

run_this_first.set_downstream(makeDecision)
build_ct_lastyear.set_upstream(makeDecision)
SkipLastYear.set_upstream(makeDecision)

extract_ct_lastyear.set_upstream(build_ct_lastyear)

build_ct_currentyear.set_upstream(extract_ct_lastyear)
build_ct_currentyear.set_upstream(SkipLastYear)

extract_ct_lastyear.set_upstream(build_ct_currentyear)
step1.set_upstream(extract_ct_currentyear)
step2.set_upstream(step1)
step3.set_upstream(step2)
