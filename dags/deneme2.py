# from airflow import DAG
# from airflow.operators.empty import EmptyOperator
# from airflow.models.baseoperator import chain
# from airflow.utils.task_group import TaskGroup

# from google.cloud import bigquery
# from datetime import datetime

# from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
#     KubernetesPodOperator,
# )
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
#     table_list_staging = ["asofcalendar_dm","calendar_dm", "service_attribute_appointment_detail_stg","appointment_survey_dm", 
#                           "appointment_replacement_stg","availability_absenteesim_stg","appointment_pay_stg","appointment_request_stg",
#                           "credit_spent_stg","partner_booking_stg","previous_day_stg","previous_day_outbound_stg","service_attribute_stg",
#                           "rescheduled_appointments_detail_stg","checkout_reconciliation_stg","subscription_dm"]
                           
        
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


#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This is an example dag for using the KubernetesPodOperator.
"""
from __future__ import annotations

import os
from datetime import datetime

from kubernetes.client import models as k8s

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# [START howto_operator_k8s_cluster_resources]
secret_file = Secret("volume", "/etc/sql_conn", "airflow-secrets", "sql_alchemy_conn")
secret_env = Secret("env", "SQL_CONN", "airflow-secrets", "sql_alchemy_conn")
secret_all_keys = Secret("env", None, "airflow-secrets-2")
volume_mount = k8s.V1VolumeMount(
    name="test-volume", mount_path="/root/mount_file", sub_path=None, read_only=True
)

configmaps = [
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="test-configmap-1")),
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="test-configmap-2")),
]

volume = k8s.V1Volume(
    name="test-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="test-volume"),
)

port = k8s.V1ContainerPort(name="http", container_port=80)

init_container_volume_mounts = [
    k8s.V1VolumeMount(mount_path="/etc/foo", name="test-volume", sub_path=None, read_only=True)
]

init_environments = [k8s.V1EnvVar(name="key1", value="value1"), k8s.V1EnvVar(name="key2", value="value2")]

init_container = k8s.V1Container(
    name="init-container",
    image="ubuntu:16.04",
    env=init_environments,
    volume_mounts=init_container_volume_mounts,
    command=["bash", "-cx"],
    args=["echo 10"],
)

affinity = k8s.V1Affinity(
    node_affinity=k8s.V1NodeAffinity(
        preferred_during_scheduling_ignored_during_execution=[
            k8s.V1PreferredSchedulingTerm(
                weight=1,
                preference=k8s.V1NodeSelectorTerm(
                    match_expressions=[
                        k8s.V1NodeSelectorRequirement(key="disktype", operator="In", values=["ssd"])
                    ]
                ),
            )
        ]
    ),
    pod_affinity=k8s.V1PodAffinity(
        required_during_scheduling_ignored_during_execution=[
            k8s.V1WeightedPodAffinityTerm(
                weight=1,
                pod_affinity_term=k8s.V1PodAffinityTerm(
                    label_selector=k8s.V1LabelSelector(
                        match_expressions=[
                            k8s.V1LabelSelectorRequirement(key="security", operator="In", values="S1")
                        ]
                    ),
                    topology_key="failure-domain.beta.kubernetes.io/zone",
                ),
            )
        ]
    ),
)

tolerations = [k8s.V1Toleration(key="key", operator="Equal", value="value")]

# [END howto_operator_k8s_cluster_resources]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_kubernetes_operator"

with DAG(
    dag_id="example_kubernetes_operator",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["example"],
) as dag:
    k = KubernetesPodOperator(
        namespace="default",
        image="ubuntu:16.04",
        cmds=["bash", "-cx"],
        arguments=["echo", "10"],
        labels={"foo": "bar"},
        secrets=[secret_file, secret_env, secret_all_keys],
        ports=[port],
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_from=configmaps,
        name="airflow-test-pod",
        task_id="task",
        affinity=affinity,
        is_delete_operator_pod=True,
        hostnetwork=False,
        tolerations=tolerations,
        init_containers=[init_container],
        priority_class_name="medium",
    )

    # [START howto_operator_k8s_private_image]
    quay_k8s = KubernetesPodOperator(
        namespace="default",
        image="quay.io/apache/bash",
        image_pull_secrets=[k8s.V1LocalObjectReference("testquay")],
        cmds=["bash", "-cx"],
        arguments=["echo", "10", "echo pwd"],
        labels={"foo": "bar"},
        name="airflow-private-image-pod",
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="task-two",
        get_logs=True,
    )
    # [END howto_operator_k8s_private_image]

    # [START howto_operator_k8s_write_xcom]
    write_xcom = KubernetesPodOperator(
        namespace="default",
        image="alpine",
        cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
        name="write-xcom",
        do_xcom_push=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="write-xcom",
        get_logs=True,
    )

    pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('write-xcom')[0] }}\"",
        task_id="pod_task_xcom_result",
    )

    write_xcom >> pod_task_xcom_result
    # [END howto_operator_k8s_write_xcom]

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)