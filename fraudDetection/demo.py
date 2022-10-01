import logging
import os
import json
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from google.cloud import storage
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.state import State
from os import listdir
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from airflow.models import Variable
from airflow.models import TaskInstance
from airflow.utils.trigger_rule import TriggerRule
# from airflow.operators import TriggerDagRunOperator
from application.helper.FRONERI_CH_Helper import csv_qa, generate_criteria, \
    get_bq_client, gcs2bq_all, generate_schemas, get_storage_client, delete_GCS_objects, move_GCS_objects, bq_qa_check, \
    check_files_for_download, ftps_loads, ftps_delete_remote_files, \
    send_message_to_pubsub, notify_failure, send_message_to_pubsub_processing

# =============================================
# ================ DAG Config =================
# =============================================

DAG_NAME = 'froneri_ingest_ch'
operation_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def catch_failure(context):
    if not context['task'].task_id == 'end':
        project_id = context['ti'].xcom_pull(key='project_id')
        bucket_data_home = context['ti'].xcom_pull(key='bucket_data_home')
        notify_failure(context, project_id, bucket_data_home, DAG_NAME)


str_dt = datetime(2022, 7, 6)
sch_intvl = '00 5 * * *'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': str_dt,
    'email': ['xxxxxx@nomad.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'operation_root': operation_root,
    'remote_ftp_port': 990,
    'local_data_home': '/home/airflow/gcs/data',
    "on_failure_callback": catch_failure
}

# =============================================
# =========== DAG Functions ===============
# =============================================

criteria_home = os.path.join(default_args['operation_root'], 'quality/qa_froneri_ch.json')
schemas_home = os.path.join(default_args['operation_root'], 'schema/froneri_ch/')


def start_func(run_id, **kwargs):
    # Storing configuration info for the particular DAG run with run_id
    if kwargs['dag_run'].conf:
        project_id = kwargs['dag_run'].conf.get('project_id', None)
    else:
        project_id = Variable.get('project_id')
    logging.info("project_id:" + project_id)
    kwargs['ti'].xcom_push(key='project_id', value=project_id)
    logging.info("dag project_id Pull: " + kwargs['ti'].xcom_pull(key='project_id'))

    if kwargs['dag_run'].conf:
        bucket_data_home = kwargs['dag_run'].conf.get('bucket_data_home')
    else:
        bucket_data_home = Variable.get(DAG_NAME + '_bucket_data_home')
    logging.info("dag bucket_data_home:" + bucket_data_home)
    kwargs['ti'].xcom_push(key='bucket_data_home', value=bucket_data_home)
    logging.info("dag bucket_data_home Pull: " + kwargs['ti'].xcom_pull(key='bucket_data_home'))

    if kwargs['dag_run'].conf:
        bq_dataset = kwargs['dag_run'].conf.get('bq_dataset')
    else:
        bq_dataset = Variable.get(DAG_NAME + '_bq_dataset')
    logging.info("dag bq_dataset:" + bq_dataset)
    kwargs['ti'].xcom_push(key='bq_dataset', value=bq_dataset)
    logging.info("dag bq_dataset Pull: " + kwargs['ti'].xcom_pull(key='bq_dataset'))

    if kwargs['dag_run'].conf:
        overwrite_flag = kwargs['dag_run'].conf.get('overwrite_flag')
    else:
        overwrite_flag = Variable.get(DAG_NAME + '_overwrite_flag')
    kwargs['ti'].xcom_push(key='overwrite_flag', value=overwrite_flag)
    logging.info("dag overwrite_flag:" + overwrite_flag)
    logging.info("dag overwrite_flag Pull: " + kwargs['ti'].xcom_pull(key='overwrite_flag'))

    if kwargs['dag_run'].conf:
        Delete_FTP_files = kwargs['dag_run'].conf.get('Delete_FTP_files')
    else:
        Delete_FTP_files = Variable.get(DAG_NAME + '_Delete_FTP_files')
    kwargs['ti'].xcom_push(key='froneri_ch_Delete_FTP_Files', value=Delete_FTP_files)
    logging.info("dag Delete_FTP_files:" + Delete_FTP_files)
    logging.info("dag Delete_FTP_files Pull: " + kwargs['ti'].xcom_pull(key='froneri_ch_Delete_FTP_Files'))

    credentials_file = Variable.get('DIP_Froneri_CH_FTP_Credentials')
    print(credentials_file)
    credentials_dict = json.loads(credentials_file)
    for key, value in credentials_dict.items():
        if key == "FTP_Address":
            remote_ftp_address = value
            kwargs['ti'].xcom_push(key='froneri_ch_address', value=remote_ftp_address)
            logging.info("dag froneri_ch_address:" + remote_ftp_address)
        elif key == "username":
            remote_ftp_username = value
            kwargs['ti'].xcom_push(key='froneri_ch_user', value=remote_ftp_username)
            logging.info("dag froneri_ch_user:" + remote_ftp_username)
        elif key == "password":
            remote_ftp_password = value
            kwargs['ti'].xcom_push(key='froneri_ch_pass', value=remote_ftp_password)
            logging.info("dag froneri_ch_password:" + remote_ftp_password)
    '''
    if kwargs['dag_run'].conf:
        username = kwargs['dag_run'].conf.get('froneri_ch_user')
    else:
        username = Variable.get(DAG_NAME + '_user')
    kwargs['ti'].xcom_push(key='froneri_ch_user', value=username)
    logging.info("dag froneri_ch_user:" + username)


    if kwargs['dag_run'].conf:
        password = kwargs['dag_run'].conf.get('froneri_ch_pass')
    else:
        password = Variable.get(DAG_NAME + '_pass')
    kwargs['ti'].xcom_push(key='froneri_ch_pass', value=password)
    logging.info("dag froneri_ch_password:" + password)


    if kwargs['dag_run'].conf:
        address = kwargs['dag_run'].conf.get('froneri_ch_address')
    else:
        address = Variable.get(DAG_NAME + '_address')
    kwargs['ti'].xcom_push(key='froneri_ch_address', value=address)
    logging.info("dag froneri_ch_address:" + address)
    logging.info("dag froneri_ch_address Pull: " + kwargs['ti'].xcom_pull(key='froneri_ch_address'))
    '''

    logging.info("run id:" + run_id)

    logging.info("dag_criteria_path:" + criteria_home)
    logging.info("dag_schemas_path:" + schemas_home)

    delete_GCS_objects(kwargs['ti'].xcom_pull(key='project_id'),
                       kwargs['ti'].xcom_pull(key='bucket_data_home'),
                       'csv/processing/')
    delete_GCS_objects(kwargs['ti'].xcom_pull(key='project_id'),
                       kwargs['ti'].xcom_pull(key='bucket_data_home'),
                       'csv/error/')

    # Sending start notification to Webex
    if "manual" in kwargs['dag_run'].run_id:
        run_type = "manually"
    else:
        run_type = "on schedule"
    if kwargs['dag_run'].conf:
        route = kwargs['dag_run'].conf.get('route')
    else:
        route = "full"
    start_date = datetime.now()
    logging.info("Slected Route:" + route)
    send_message_to_pubsub(kwargs['ti'].xcom_pull(key='project_id'), DAG_NAME, "PROC_INFO", "started", str(start_date),
                           run_type, route)


def route_branch_func(run_id, **kwargs):
    if kwargs['dag_run'].conf:
        selected_route = kwargs['dag_run'].conf.get('route')
    else:
        selected_route = 'full'

    kwargs['ti'].xcom_push(key='selected_route', value=selected_route)
    logging.info("dag selected_route:" + selected_route)
    logging.info("dag selected_route Pull: " + kwargs['ti'].xcom_pull(key='selected_route'))

    if selected_route == 'full':
        return 'route_full'
    elif selected_route == 'csv':
        return 'route_csv'
    else:
        return 'route_unknown'


def check_ftps_load_func(run_id, **kwargs):
    remote_ftp_address = kwargs['ti'].xcom_pull(key='froneri_ch_address')
    remote_ftp_username = kwargs['ti'].xcom_pull(key='froneri_ch_user')
    remote_ftp_port = default_args['remote_ftp_port']
    remote_ftp_password = kwargs['ti'].xcom_pull(key='froneri_ch_pass')

    list_files_to_download = check_files_for_download(remote_ftp_address, remote_ftp_username, remote_ftp_password,
                                                      remote_ftp_port, kwargs['ti'].xcom_pull(key='overwrite_flag'))

    if len(list_files_to_download) == 0:
        print('There is no new file in FTPS folder : {}'.format(remote_ftp_address))
        raise ValueError("There is no new file in FTPS folder")
    else:
        print('There are new files to load in FTPS folder {} : {}'.format(remote_ftp_address, list_files_to_download))
        return ['FTPS_Load', 'finishing']


def ftps_load_func(run_id, **kwargs):
    remote_ftp_address = kwargs['ti'].xcom_pull(key='froneri_ch_address')
    remote_ftp_username = kwargs['ti'].xcom_pull(key='froneri_ch_user')
    remote_ftp_port = default_args['remote_ftp_port']
    remote_ftp_password = kwargs['ti'].xcom_pull(key='froneri_ch_pass')

    list_files_to_download = check_files_for_download(remote_ftp_address, remote_ftp_username, remote_ftp_password,
                                                      remote_ftp_port, kwargs['ti'].xcom_pull(key='overwrite_flag'))

    if len(list_files_to_download) == 0:
        print('There is no file in FTPS folder : {}'.format(remote_ftp_address))
    else:
        print('There are new files to load in FTPS folder {} : {}'.format(remote_ftp_address, list_files_to_download))
        ftps_loads(kwargs['ti'].xcom_pull(key='project_id'), kwargs['ti'].xcom_pull(key='bucket_data_home'),
                   list_files_to_download, remote_ftp_address, remote_ftp_username, remote_ftp_password,
                   remote_ftp_port, default_args['local_data_home'])


def csv_qa_check_func(run_id, **kwargs):
    storage_client = storage.Client(kwargs['ti'].xcom_pull(key='project_id'), None)
    bucket = storage_client.bucket(kwargs['ti'].xcom_pull(key='bucket_data_home'))

    # Check if processing or error csv folders are empty otherwise delete the daily csv file
    # delete_GCS_object usage delete_GCS_objects(project_id,bucket_uri,blob_uri)
    delete_GCS_objects(kwargs['ti'].xcom_pull(key='project_id'),
                       kwargs['ti'].xcom_pull(key='bucket_data_home'),
                       'csv/processing/')
    delete_GCS_objects(kwargs['ti'].xcom_pull(key='project_id'),
                       kwargs['ti'].xcom_pull(key='bucket_data_home'),
                       'csv/error/')

    # File exists and QA checks on pending csv folder
    blobs = bucket.list_blobs(prefix='csv/pending/')
    for blob_pending in blobs:
        logging.info("blob_pending :" + blob_pending.name)
        if not blob_pending.name.endswith('/'):
            logging.info("blob_pending :" + blob_pending.name)
            new_blob_name = blob_pending.name.replace(' ', '_')
            new_blob = bucket.rename_blob(blob_pending, new_blob_name)
            print("Blob {} has been renamed to {}".format(blob_pending.name, new_blob.name))
            blob_from = new_blob.name.split('/')[0] + '/' + new_blob.name.split('/')[1] + '/' + \
                        new_blob.name.split('/')[2]
            if not csv_qa(new_blob, generate_criteria(criteria_home)):
                move_GCS_objects(kwargs['ti'].xcom_pull(key='project_id'),
                                 kwargs['ti'].xcom_pull(key='bucket_data_home'),
                                 blob_from,
                                 ('csv/error/'))
                raise ValueError(
                    'The size of the data file {} is less than the criteria defined in the QA file {}'.format(
                        blob_pending.name, criteria_home))

            else:
                move_GCS_objects(kwargs['ti'].xcom_pull(key='project_id'),
                                 kwargs['ti'].xcom_pull(key='bucket_data_home'),
                                 blob_from,
                                 ('csv/processing/'))


def bq_load_func(run_id, **kwargs):
    storage_client = get_storage_client(kwargs['ti'].xcom_pull(key='project_id'), None)
    bucket = storage_client.bucket(kwargs['ti'].xcom_pull(key='bucket_data_home'), None)
    bq_client = get_bq_client(kwargs['ti'].xcom_pull(key='project_id'), None)
    blobs = bucket.list_blobs(prefix='csv/processing/')
    delimeter = ","
    for blob_processing in blobs:
        if not blob_processing.name.endswith('/'):
            file_name = str(blob_processing.name.split('/')[2])
            logging.info("BQ table Name: {}".format(file_name))
            schemas_home_override = os.path.join(default_args['operation_root'],
                                                 'schema/froneri_ch/')
            schemas, table_name = generate_schemas(schemas_home_override,
                                                   file_name.replace("Cognos", "DailySales_Cognos"))
            bq_table_uri = kwargs['ti'].xcom_pull(key='bq_dataset') + '.' + table_name.split('_')[0]
            gcs_file_uri = "gs://{0}/{1}".format(kwargs['ti'].xcom_pull(key='bucket_data_home'), blob_processing.name)
            logging.info("Big Query Table: {}".format(bq_table_uri))
            logging.info("GCS file Path: {}".format(gcs_file_uri))
            bq_load_summary = gcs2bq_all(bq_table_uri, gcs_file_uri, bq_client, schemas, delimeter, 'write_append')

            kwargs['ti'].xcom_push(key="bq_load_summary_" + table_name.split('.')[0] + '_' + run_id,
                                   value=bq_load_summary)


def bq_load_qa_func(run_id, **kwargs):
    bq_load_result = dict()
    bq_client = get_bq_client(kwargs['ti'].xcom_pull(key='project_id'), None)
    storage_client = get_storage_client(kwargs['ti'].xcom_pull(key='project_id'), None)
    bucket = storage_client.bucket(kwargs['ti'].xcom_pull(key='bucket_data_home'), None)
    blobs = bucket.list_blobs(prefix='csv/processing/')
    for blob_processing in blobs:
        if not blob_processing.name.endswith('/'):
            file_name = str(blob_processing.name.split('/')[2])
            table_name = file_name.split('.')[0]

            query = f"SELECT table_name FROM {kwargs['ti'].xcom_pull(key='bq_dataset')}.INFORMATION_SCHEMA.TABLES where table_name like '{table_name}%'"
            query_job_tables = bq_client.query(query)
            for row_tables in query_job_tables:
                if not row_tables[0].endswith('_latest'):
                    table_name = row_tables[0]
                    bq_qa_criteria = generate_criteria(criteria_home)['table']['size']
                    print(table_name)
                    print(run_id)
                    bq_load_summary = kwargs['ti'].xcom_pull(key="bq_load_summary_" + table_name + '_' + run_id)
                    logging.info("BQ ingestion summary: {}".format(bq_load_summary))
                    bq_ingest_fail, bq_ingest_success = bq_qa_check(bq_load_result, bq_load_summary, bq_qa_criteria)
                    if bq_ingest_fail:
                        raise ValueError(
                            'The size of the table {} is less than the criteria defined in the file'.format(table_name))


def view_update_func(run_id, **kwargs):
    bq_client = get_bq_client(kwargs['ti'].xcom_pull(key='project_id'), None)
    for file in listdir(schemas_home):
        file = file.replace('.json', '')
        query = f"SELECT table_name FROM {kwargs['ti'].xcom_pull(key='bq_dataset')}.INFORMATION_SCHEMA.TABLES where table_name like '%{file}%'"
        query_job_tables = bq_client.query(query)
        results = query_job_tables.result()

        if results.total_rows == 0:
            print('There is no data for dataset {}'.format(schemas_home))
            continue
        else:
            view_name = file + '_latest_v1'
            view_id = "{}.{}.{}".format(kwargs['ti'].xcom_pull(key='project_id'),
                                        kwargs['ti'].xcom_pull(key='bq_dataset'), view_name)
            table_id = "{}.{}.{}".format(kwargs['ti'].xcom_pull(key='project_id'),
                                         kwargs['ti'].xcom_pull(key='bq_dataset'),
                                         file)

            try:
                bq_client.get_table(view_id)  # Make an API request.
                logging.info(
                    "View {0} already exists. Will delete and replace it with new table {1}!".format(view_id,
                                                                                                     table_id))
                bq_client.delete_table(view_id)
            except NotFound:
                logging.info(
                    "View {0} does not exist and will create it for table {1}".format(view_id, table_id))

            view = bigquery.Table(view_id)

            view.view_query = f"SELECT * FROM `{table_id}`"
            view = bq_client.create_table(view)
            logging.info(f"Created View {view.table_type}: {str(view.reference)}")


def end_func(run_id, ds_nodash, **kwargs):
    remote_ftp_port = default_args['remote_ftp_port']

    storage_client = storage.Client(kwargs['ti'].xcom_pull(key='project_id'), None)
    bucket = storage_client.bucket(kwargs['ti'].xcom_pull(key='bucket_data_home'))
    blobs = bucket.list_blobs(prefix='csv/processing/')
    for blob_pending in blobs:
        print("blob_pending is " + blob_pending.name)
        if not blob_pending.name.endswith('/'):
            blob_from = blob_pending.name.split('/')[0] + '/' + blob_pending.name.split('/')[1] + '/' + \
                        blob_pending.name.split('/')[2]
            print("blob_from is " + blob_from)
            move_GCS_objects(kwargs['ti'].xcom_pull(key='project_id'),
                             kwargs['ti'].xcom_pull(key='bucket_data_home'),
                             blob_from,
                             'csv/archive/')

    # Cleaning up the pending and error csv folders except arhive
    # delete_GCS_object usage delete_GCS_objects(project_id,bucket_uri,blob_uri)
    delete_GCS_objects(kwargs['ti'].xcom_pull(key='project_id'),
                       kwargs['ti'].xcom_pull(key='bucket_data_home'),
                       'csv/pending/')
    delete_GCS_objects(kwargs['ti'].xcom_pull(key='project_id'),
                       kwargs['ti'].xcom_pull(key='bucket_data_home'),
                       'csv/error/')


    credentials_file = Variable.get('DIP_Froneri_CH_FTP_Credentials')
    print(credentials_file)
    credentials_dict = json.loads(credentials_file)
    for key, value in credentials_dict.items():
        if key == "FTP_Address":
            remote_ftp_address = value
            kwargs['ti'].xcom_push(key='froneri_ch_address', value=remote_ftp_address)
            logging.info("dag froneri_ch_address:" + remote_ftp_address)
        elif key == "username":
            remote_ftp_username = value
            kwargs['ti'].xcom_push(key='froneri_ch_user', value=remote_ftp_username)
            logging.info("dag froneri_ch_user:" + remote_ftp_username)
        elif key == "password":
            remote_ftp_password = value
            kwargs['ti'].xcom_push(key='froneri_ch_pass', value=remote_ftp_password)
            logging.info("dag froneri_ch_password:" + remote_ftp_password)

    list_files_to_download = check_files_for_download(remote_ftp_address, remote_ftp_username, remote_ftp_password,remote_ftp_port, kwargs['ti'].xcom_pull(key='overwrite_flag'))

    ftps_delete_remote_files(kwargs['ti'].xcom_pull(key='project_id'),
                                 kwargs['ti'].xcom_pull(key='bucket_data_home'), list_files_to_download,
                                 remote_ftp_address, remote_ftp_username, remote_ftp_password, remote_ftp_port)

    Success_flag = True
    for task_instance in kwargs['dag_run'].get_task_instances():
        if task_instance.current_state() == State.FAILED and \
                task_instance.task_id != kwargs['task_instance'].task_id:
            TaskInstance(kwargs['dag'].get_task("end"), kwargs['execution_date']).set_state(State.FAILED)
            Success_flag = False
    if Success_flag:
        end_date = datetime.now()
        send_message_to_pubsub(kwargs['ti'].xcom_pull(key='project_id'), DAG_NAME, "PROC_INFO", "finished",
                               str(end_date))
        dag_instance = kwargs['dag']
        operator_instance = dag_instance.get_task("Bq_load")
        task_status = TaskInstance(operator_instance, kwargs['execution_date']).current_state()
        if task_status == State.SUCCESS:
            send_message_to_pubsub(kwargs['ti'].xcom_pull(key='project_id'), DAG_NAME, "PROC_LAN",
                                   "new data landed succesfully", str(end_date))
            project_id = kwargs['ti'].xcom_pull(key='project_id')
            send_message_to_pubsub_processing(project_id, DAG_NAME)


# =============================================
# =========== DAG Execution ===============
# =============================================
with DAG(dag_id=DAG_NAME, schedule_interval=sch_intvl, default_args=default_args) as dag:
    start = PythonOperator(task_id='start', provide_context=True, python_callable=start_func, dag=dag)

    branch_op = BranchPythonOperator(task_id='branching', provide_context=True, trigger_rule="all_done",
                                     python_callable=route_branch_func, dag=dag)

    route_full = DummyOperator(task_id='route_full', dag=dag)

    route_unknown = DummyOperator(task_id='route_unknown', trigger_rule='none_failed', dag=dag)

    route_csv = DummyOperator(task_id='route_csv', trigger_rule='none_failed', dag=dag)

    check_ftps_load = BranchPythonOperator(task_id='Check_For_New_FTPS_files', trigger_rule=TriggerRule.ONE_SUCCESS,
                                           provide_context=True, python_callable=check_ftps_load_func, dag=dag)

    ftps_load = PythonOperator(task_id='FTPS_Load', trigger_rule=TriggerRule.ONE_SUCCESS, provide_context=True,
                               python_callable=ftps_load_func, dag=dag)

    csv_qa_check = PythonOperator(task_id='Csv_qa_check', trigger_rule=TriggerRule.ONE_SUCCESS, provide_context=True,
                                  python_callable=csv_qa_check_func, dag=dag)

    bq_load = PythonOperator(task_id='Bq_load', trigger_rule='all_success', provide_context=True,
                             python_callable=bq_load_func, dag=dag)

    bq_load_qa = PythonOperator(task_id='Bq_qa_check', trigger_rule='all_success', provide_context=True,
                                python_callable=bq_load_qa_func, dag=dag)

    view_update = PythonOperator(task_id='View_update', trigger_rule='all_success', provide_context=True,
                                 python_callable=view_update_func, dag=dag)

    finishing = DummyOperator(task_id='finishing', trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

    end = PythonOperator(task_id='end', trigger_rule='all_success', provide_context=True, python_callable=end_func,
                         dag=dag)
    '''
    trigger = TriggerDagRunOperator(
        task_id="Data_Processing_trigger_dagrun",
        trigger_rule='all_success',
        trigger_dag_id="Froneri_CH_Data_Processing",  # Ensure this equals the dag_id of the DAG to trigger
        conf={"message": "DPP Triggered"},
    )
    '''

start >> branch_op >> [route_full, route_csv, route_unknown]

route_full >> check_ftps_load
route_csv >> csv_qa_check >> bq_load >> bq_load_qa >> view_update >> finishing
route_unknown >> finishing

check_ftps_load >> ftps_load
ftps_load >> csv_qa_check >> bq_load >> bq_load_qa >> view_update >> finishing
# finishing >> end >> trigger
finishing >> end