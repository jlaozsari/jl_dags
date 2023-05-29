import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
}

dag = DAG('sampleDAG', default_args=default_args, schedule_interval=None)


DriftTask = KubernetesPodOperator(
    # namespace='default',
    image="apache/airflow:2.5.3",
    cmds=["python", "-c"],
    arguments=["print('This code is running in a Kubernetes Pod')"],
    labels={},
    name="sampleDAG",
    task_id="sampleDAG",
    get_logs=True,
    dag=dag,
    log_events_on_failure=True,
    replicas=1,
    is_delete_operator_pod=True)


DriftTask