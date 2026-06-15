import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_DP_FOLDER_ID = 'b1ghvklfff3gheghai4a'
YC_DP_SSH_PUBLIC_KEY = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQC8bm4GD6uTp6f3/uNtbbdDREsQeput3ktkCcazrypAtgGNdjVp0gJHKZwk9zVz8aeYey2V9hbvupYig1fa1AmLusw26EZBUmS++WyHxnvw4PZ3qsredctkBw+1RVgEUKMYVPBKV8M7C6Yw2PPpoBjGCALnyAOCillYrf/v8Ot0tn7KVmZjQY2H6FyG/OeXdhzyotk77agneDnuHYB/pnfgylBbYwVazK62byAip+TkbiWeMKVrgSZySYchVAKKxiyjDvYirqHmGj3RS2E6jZUsoCFrm+vihFcb6LixrKLsERcszjjGOsIEuaijhsOJiYZCitezywpYYFxbrLw3bRCB+1tN4QcNO5naCV5lRoXEeldW+UoGhY7oEk1klJYFp/IlVt0xcdKQSaU/naIl1cX2P9PFR3EOF3FKTT3g59IBU1SOKqG/QNfBTLmeuJ+Iu9E5LkePbJB6TJtRhFGt6RIV0gyfnqTzE1QETRnyx/MYkLzWRYWRkP7vBPhWdWoshMLQ2scPBqHH41rL2z2XfqMxzRD+0UmyIEqmx0NDYqP5lMO+o0HHtSTxxEdxJJFMUSXfGmcknOOGTgUlsolwimw4G7cttI9eJ9IxFHte1uZJmXzjHBlzGMDRgtSlMiP2LJBt3doU1C6WZ8q175KagyqlbWDFobKXhh2jzPNjG66BaQ=='
YC_DP_SUBNET_ID = 'e9btf2hr7o3fffrk7ju6'
YC_DP_SA_ID = 'aje6dpgbj7ro5c1idj6j'
YC_DP_AZ = 'ru-central1-a'
YC_SOURCE_BUCKET = '2-task' 
YC_DP_LOGS_BUCKET = '2-task-output' 


with DAG(
    'DATA_INGEST',
    schedule='@hourly',
    tags=['data-processing-and-airflow', 'yandex', 'dataproc', 'pyspark'],
    start_date=datetime.datetime.now(),
    max_active_runs=1,
    catchup=False,
    description="Создание кластера Data Proc, запуск PySpark-задачи, удаление кластера",
) as ingest_dag:

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        folder_id=YC_DP_FOLDER_ID,
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Временный кластер для выполнения PySpark-задания под оркестрацией Managed Service for Apache Airflow™',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_DP_LOGS_BUCKET,
        service_account_id=YC_DP_SA_ID,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.micro',
        masternode_disk_type='network-ssd',
        masternode_disk_size=20,
        computenode_resource_preset='s2.micro',
        computenode_disk_type='network-ssd',
        computenode_disk_size=10,
        computenode_count=2,
        computenode_max_hosts_count=5,
        services=['YARN', 'SPARK'],
        datanode_count=0,
        connection_id='yandexcloud_default',
    )

    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/scripts/job.py',
        args=[
            f's3a://{YC_SOURCE_BUCKET}/data/applications.csv',
            f's3a://{YC_DP_LOGS_BUCKET}/results/'
        ],
        connection_id='yandexcloud_default',
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
        connection_id='yandexcloud_default',
    )

    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster