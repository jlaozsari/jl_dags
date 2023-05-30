# from airflow import DAG
# from airflow.operators.empty import EmptyOperator
# from airflow.models.baseoperator import chain
# from airflow.utils.task_group import TaskGroup

# from google.cloud import bigquery
# from datetime import datetime

# from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# from kubernetes.client import models as k8s_models


# def build_tasks(table_list):
#     tasks = []
#     for table in table_list:
#         task_id = table + "_task"
#         table_type_ = table.split("_")[-1]
#         task = KubernetesPodOperator(
#                 task_id=task_id,
#                 name=task_id,
#                 namespace="airflow",
#                 image="apache/airflow:2.6.1", #  gcr.io/gcp-runtimes/ubuntu_20_0_4
#                 # cmds=["python3", "/app/bigquery-workflow.py"],
#                 cmds=["echo"],
#                 arguments=[
#                     "--table", table,
#                     "--table_type", table_type_
#                 ],
#                 container_resources=k8s_models.V1ResourceRequirements(
#                     limits={"memory": "1Gi", "cpu": "1000m"},
#                 ),
#                 config_file="/home/airflow/composer_kube_config", # kubernetespodoperator
#                 kubernetes_conn_id="kubernetes_default",
#                 do_xcom_push=False,
#                 # task_concurrency=12
#                 )
#     #     KubernetesPodOperator(
#     #     # The ID specified for the task.
#     #     task_id="pod-ex-minimum",
#     #     # Name of task you want to run, used to generate Pod ID.
#     #     name="pod-ex-minimum",
#     #     # Entrypoint of the container, if not specified the Docker container's
#     #     # entrypoint is used. The cmds parameter is templated.
#     #     cmds=["echo"],
#     #     # The namespace to run within Kubernetes, default namespace is
#     #     # `default`. In Composer 1 there is the potential for
#     #     # the resource starvation of Airflow workers and scheduler
#     #     # within the Cloud Composer environment,
#     #     # the recommended solution is to increase the amount of nodes in order
#     #     # to satisfy the computing requirements. Alternatively, launching pods
#     #     # into a custom namespace will stop fighting over resources,
#     #     # and using Composer 2 will mean the environment will autoscale.
#     #     namespace="default",
#     #     # Docker image specified. Defaults to hub.docker.com, but any fully
#     #     # qualified URLs will point to a custom repository. Supports private
#     #     # gcr.io images if the Composer Environment is under the same
#     #     # project-id as the gcr.io images and the service account that Composer
#     #     # uses has permission to access the Google Container Registry
#     #     # (the default service account has permission)
#     #     image="gcr.io/gcp-runtimes/ubuntu_18_0_4",
#     # )
#         tasks.append(task)
#     return tasks

# with DAG("bigquery_workflow", start_date=datetime(2022, 1, 1),
#          schedule_interval="20 01 * * *", catchup=False) as dag:
    
#     start = EmptyOperator(task_id="start")

#     ### eger marketing dm ve oncesi calisacak bir sql yazdiysak buraya cift tirnak icinde eklememiz yeterli oluyor. Otomatik olarak task tanimlanacaktir.
#     table_list_staging = ["asofcalendar_dm","calendar_dm", "service_attribute_appointment_detail_stg"]

#     # ,"appointment_survey_dm", 
#     #                       "appointment_replacement_stg","availability_absenteesim_stg","appointment_pay_stg","appointment_request_stg",
#     #                       "credit_spent_stg","partner_booking_stg","previous_day_stg","previous_day_outbound_stg","service_attribute_stg",
#     #                       "rescheduled_appointments_detail_stg","checkout_reconciliation_stg","subscription_dm"
                           
        
#     ### appointment_dm den sonra calisacak bir task varsa buraya ekleyebilirsin.
#     # horiz_list_one= ["policy_popup_reschedule_stg","driver_appointment_details_stg","marketing_stg","marketing_dm",
#     #                  "appointment_type_dm","appointment_stg","appointment_dm"]
        
#     # ### cogs yapisinda bir ekleme yapmak istersen buraya ekleyebilirsin.
#     # table_list_cogs= ["voucher_dm", "cogs_dtc_stg","cogs_fixed_fee_ksa_stg","cogs_fixed_fee_stg","cogs_min_guar_success_stg"]
        
#     # ### finance tablolarina bir tablo eklemek istersen buraya ekleyebilirsin.
#     # table_list_finance = ["cogs_distribution_stg" ,"cogs_distribution_to_gmv_stg","finance_dm" ,"finance_pl_weekly_data_etl_stg",
#     #                         "finance_pl_gmv_stg","finance_pl_distribution_stg","finance_pl_stg","finance_pl_dm","pl_new_format_stg"]

#     ### build task bu tasklari birbirine otomatik olarak baglayan bir task mekanizmasi kuracaktir. Bunu yapmasaydik 30-40 tablo icin 
#     ### tek tek task tanimlamamiz gerekecekti.
#     # task_staging = build_tasks(table_list_staging)
#     with TaskGroup("staging_group") as staging_group:
#         task_staging = build_tasks(table_list_staging)
#     # with TaskGroup("first_horiz_group") as first_horiz_group:
#     #     tasks_horiz_one = build_tasks(horiz_list_one)
#     # with TaskGroup("cogs_group") as cogs_group:
#     #     tasks_cogs = build_tasks(table_list_cogs)
#     # with TaskGroup("finance_group") as finance_group:
#     #     tasks_finance = build_tasks(table_list_finance)
    

    
#     finish = EmptyOperator(task_id="finish")

#     # chain(start, task_staging, *tasks_horiz_one, tasks_cogs, *tasks_finance, finish)
#     chain(start, task_staging, finish)


from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 31),
}

with DAG("bigquery_workflow", start_date=datetime(2022, 1, 1),
         schedule_interval="20 01 * * *", catchup=False) as dag:

    run_this = KubernetesPodOperator(
        task_id='run_pod',
        name='pod-operator-example',
        namespace='default',
        image='apache/airflow:2.6.1',  # Docker imajınızı buraya ekleyin
        cmds=['echo'],  # Pod içinde çalıştırılacak komutları buraya ekleyin
        conn_id='kubernetes_default',
        is_delete_operator_pod=True,
        get_logs=True
        # arguments=['arg1', 'arg2'],  # Komutlara geçmek istediğiniz argümanları buraya ekleyin
    )

    run_this
