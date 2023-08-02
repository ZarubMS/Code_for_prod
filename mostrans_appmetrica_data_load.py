# -*- coding: utf-8 -*-
"""

"""
import os
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from custom_operators.branch_hdfs_operator import BranchHDFSOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.dummy_operator import DummyOperator



ALERT_MAILS = Variable.get("gv_ic_admin_lst")
DAG_NAME = str(os.path.basename(__file__).split('.')[0])
OWNER = 'Zarubin Maksim'
DEPENDS_ON_PAST = True
EMAIL_ON_FAILURE = True
EMAIL_ON_RETRY = False
RETRIES = int(Variable.get('gv_dag_retries'))
POOL = 'data_pool'
MAIN_VAR_NAME = 'gv_' + DAG_NAME

start_dt = pendulum.datetime(2023, 7, 25).astimezone()

# setting default arguments of dag
default_args = {
    'owner': OWNER,
    'depends_on_past': DEPENDS_ON_PAST,
    'start_date': start_dt,
    'email': ALERT_MAILS,
    'email_on_failure': EMAIL_ON_FAILURE,
    'email_on_retry': EMAIL_ON_RETRY,
    'retries': RETRIES,
    'pool': POOL
}

# Creating DAG with parameters
dag = DAG(DAG_NAME, default_args=default_args, schedule_interval="26 16 * * *", max_active_runs=1)
dag.doc_md = __doc__

entities = {
    'mostrans_appmetrica': {
        'sftp_dir': '/home/mzarubin/Code/files/mostrans_appmetrica/',
        'hdfs_raw_dir': 'source_19_mosgorpass/files/mostrans_appmetrica/',
        'file_pattern': 'mostrans_appmetrica',
        'hdfs_dir': '/apps/hive/warehouse/mosgorpass_stg.db/mostrans_appmetrica/',
        'hive': 'mosgorpass_data.mostrans_appmetrica',  
        'hive_stg': 'mosgorpass_stg.mostrans_appmetrica',
        'data_fields': 'to_date(date1),'
                       'to_date(date2),'
                       'cast(time_intervals as timestamp),'
                       'id,'
                       'metric_nm,'
                       'cast(metric_val as float),'
    }
}

file_pattern_suffix = '''_{{ macros.tzu.ds_nodash(ti) }}.csv.gz'''

mosgorpass_appmetrica_copy_cmd = """
file_pattern='{{ params.file_pattern }}%s'
hdfs dfs -rm -r -skipTrash {{ params.hdfs_dir }}*
hdfs dfs -cp {{ params.hdfs_raw }}${file_pattern} {{ params.hdfs_dir }}${file_pattern}
""" % file_pattern_suffix


mosgorpass_appmetrica_load_sql = """
spark-sql --master yarn  \
--name "mosgorpass_appmetrica" \
--conf "spark.hadoop.hive.cli.print.header=true" \
--conf "spark.hadoop.hive.exec.dynamic.partition=true" \
--conf "spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict" \
--conf spark.executor.cores=5 \
--conf spark.executor.memory=2G \
-e \
'
INSERT INTO {{ params.hive }} PARTITION (period_from_dt, period_to_dt)
SELECT DISTINCT
   {{ params.data_fields }}, 
   current_timestamp() as process_dttm,
   {{ params.part_field }} AS period_from_dt, 
   {{ params.part_field }} AS period_to_dt,
FROM {{ params.hive_stg }}
DISTRIBUTE BY period_from_dt, period_to_dt;'
"""

mosgorpass_appmetrica_start = DummyOperator(
    task_id='mostrans_appmetrica_start',
    dag=dag
)

for key, value in entities.items():
    fact_branch_start = BranchHDFSOperator(
        task_id='mosgorpass_' + key + '_exists',
        on_success_id='mosgorpass_' + key + '_copy',
        on_failure_id='mosgorpass_' + key + '_nofile',
        sftp_path=value['sftp_dir'],
        hdfs_raw_dir=value['hdfs_raw_dir'],
        file_pattern=value['file_pattern'] + file_pattern_suffix,
        wait_for_downstream=True,
        dag=dag
    )

    nofile = DummyOperator(
        task_id='mosgorpass_' + key + '_nofile',
        trigger_rule=TriggerRule.NONE_FAILED,
        wait_for_downstream=True,
        dag=dag
    )

    fact_branch_end = DummyOperator(
        task_id='mosgorpass_' + key + '_branch_end',
        trigger_rule=TriggerRule.NONE_FAILED,
        wait_for_downstream=True,
        dag=dag
    )

    
    mosgorpass_appmetrica_copy = BashOperator(
        task_id='mosgorpass_' + key + '_copy',
        bash_command=mosgorpass_appmetrica_copy_cmd,
        params={
            'hdfs_raw': fact_branch_start.hdfs_root + value['hdfs_raw_dir'],
            'hdfs_dir': value['hdfs_dir'],
            'file_pattern': value['file_pattern'],
        },
        wait_for_downstream=True,
        dag=dag
    )

    mosgorpass_appmetrica_load = BashOperator(
        task_id='mosgorpass_' + key + '_load',
        bash_command=mosgorpass_appmetrica_load_sql,
        params={
            'hive': value['hive'],
            'hive_stg': value['hive_stg'],
            'data_fields': value['data_fields']
        },
        wait_for_downstream=True,
        dag=dag
    )


    mosgorpass_appmetrica_start >> fact_branch_start
    fact_branch_start >> mosgorpass_appmetrica_copy
    mosgorpass_appmetrica_copy >> mosgorpass_appmetrica_load
    mosgorpass_appmetrica_load >> fact_branch_end

    fact_branch_start >> nofile
    nofile >> fact_branch_end


