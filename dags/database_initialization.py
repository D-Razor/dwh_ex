import pandas
import os
from datetime import datetime
from custom_operator.filesystem_parser import HOME_FOLDER, populate_struct, populate_db_list
from custom_operator.core_objects import Base, DBFile, DBFileVersion, DBFolder

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import List, Dict

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from custom_operator.db_init_operator import DBInitOperator
from custom_operator.structure_monitoring_operator import StructureMonitoringOperator


# LOCAL_SQLITE_URL = "sqlite:///local_database.db"
# project_engine = create_engine(LOCAL_SQLITE_URL)


args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='local_structure_dag',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
    # params = {
        # "project_engine": create_engine(LOCAL_SQLITE_URL)
    # }
) as dag:

    db_init = DBInitOperator(
        task_id='db_init',
        name='custom_db_init'
    )
    
    struct_monitor = StructureMonitoringOperator(
        task_id='struct_monitor',
        name='custom_struct_monitor'
    )
    
    def db_init_choice():
        if os.path.isfile('local_database.db'):
            return 'struct_monitor'
        else:
            return 'db_init'
    
    branching = BranchPythonOperator(
        task_id='init_branching',
        python_callable=db_init_choice
    )
    
    branching
