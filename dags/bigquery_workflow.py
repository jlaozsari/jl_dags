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
    image="europe-west1-docker.pkg.dev/justmop-262a8/justlife/python-test",
    cmds=["python", "-c"],
    arguments=["print('This code is running in a Kubernetes Pod')"],
    labels={},
    name="sampleDAG",
    task_id="sampleDAG",
    get_logs=True,
    dag=dag,
    log_events_on_failure=True,
    in_cluster=True,
    is_delete_operator_pod=True)


DriftTask
