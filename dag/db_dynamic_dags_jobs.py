import pandas as pd
import urllib
import sqlalchemy as sa
from sqlalchemy import exc
import psycopg2

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'airflow_master',
    'depends_on_past': False,
    'start_date': datetime(2022,8,25),
    'email': ['noreply@noreply.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

'''
# Read_db_elt_log_PGSQL () 
# To check if upstream ETL has run and data load is done
# This function reads db_elt_log table for records getting populated from upstream Spark job
# return lud_df => returns the records of the table as dataframe
'''

def Read_db_elt_log_PGSQL():
    
    # Reading Last Update Date and it's corresponding module_name from db_elt_log
    host = Variable.get("db_elt_log_host")
    database = Variable.get("db_elt_log_database")
    user = Variable.get("db_elt_log_user")
    password = Variable.get("db_elt_log_password")
    read_log_table = Variable.get("db_read_log_table")
    conn = None
    try:
        conn = psycopg2.connect(host=host, user=user, password=password, dbname=database, port=5432)
        cur = conn.cursor()
        sql_query = "select * from " + read_log_table
        lud_df = pd.read_sql_query(sql_query, con = conn, index_col = None)
        
        if lud_df.empty:
            print("No Update")
            return lud_df # if no records in the table, return empty dataframe
        else:
            print(lud_df)
            return lud_df # if records are in the table, return the records
        
    except psycopg2.OperationalError as e:
        #conn.close()
        print(str(e))
    finally:
        if conn:
            conn.close()
        
'''
# Create_Nodes_4_Airflow ()
# This function creates a list of module names (['TASK_1', 'TASK_2', 'TASK_3', 'TASK_4', 'TASK_5']) 
# The module names will be utilized to create nodes for the DAG by airflow
# return [] if log table is empty otherwise return list of module names (parallel_nodes)
'''
def Create_Nodes_4_Airflow(lud_df):
    
    parallel_nodes = []
    if len(lud_df) == 0: # lud_df == 0:
        return parallel_nodes
    
    number_of_nodes = len(lud_df)
    
    for i in range(number_of_nodes):
        parallel_nodes.insert(i, lud_df['module_name'][i])
    
    print("Name of nodes = ",parallel_nodes)
    return parallel_nodes

'''
# on_success_delete_log_record()
# On Successful execution of the desired Stored Procedure by PostgresOperator(), 
# it's corresponding module name will be deleted by this function from log table
'''

def on_success_delete_log_record(**op_kwargs):
    read_log_table = Variable.get("db_read_log_table")
    sql_query_ = "delete from " + read_log_table + " WHERE module_name = " + op_kwargs['module_name'] + ";"
    # Reading Last Update Date and it's corresponding module_name from db_elt_log
    host = Variable.get("db_elt_log_host")
    database = Variable.get("db_elt_log_database")
    user = Variable.get("db_elt_log_user")
    password = Variable.get("db_elt_log_password") 
    
    try:
        conn = psycopg2.connect(host=host, user=user, password=password, dbname=database, port=5432)
        cur = conn.cursor()
        cur.execute(sql_query_)
        conn.commit()
        cur.close()
        print("Module Delete is = ",op_kwargs['module_name'])
    except psycopg2.OperationalError as e:
        print(str(e))
    finally:
        if conn is not None:
            conn.close()
 
'''
# parallel_branch()
# This function decides the flow of nodes in the DAG
# if the log table is empty i.e. no module names are present
# if sends task => Start >> pb >> Empty_log_table >> End
# if log table has records then
# Start >> pb >> pb1 >> End
# pb1 => 
'''
def parallel_branch(*op_args):
    return 'parallel_branch_nodes' if len(op_args) != 0 else "No_Records_in_log_table"
    #if len(op_args) !=0:
    #    return ['call_SPs_'+a for a in parallel_nodes]
    #else:
    #    return "Dummy_No_Task"
    
    

with DAG('db_dynamic_dags_jobs', default_args=default_args, catchup=False, schedule_interval=Variable.get("db_NOTES_jobs_schedule")) as dag:
    
    last_date_update = Read_db_elt_log_PGSQL() # python_callable
    parallel_nodes = Create_Nodes_4_Airflow(last_date_update) # parallel_nodes = ['TASK_1', 'TASK_2', 'TASK_3', 'TASK_4', 'TASK_5']
    
    # 1. Start
    Start = DummyOperator(task_id = 'Start')
        
    # 2. END of Dags
    End = DummyOperator(task_id = 'End', trigger_rule = 'none_failed_or_skipped')
    
    # 3. To work when there is no recod in db_elt_log
    No_Records_in_log_table = DummyOperator(task_id = 'No_Records_in_log_table')
    
    # 4. Dummy Operator parallel_branch_nodes
    parallel_branch_nodes = DummyOperator(task_id = 'parallel_branch_nodes')
    
    # 5. Branch Operator to decide the flow of nodes
    decision_node = BranchPythonOperator(
        task_id = "decision_node", 
        python_callable=parallel_branch,
        op_args = parallel_nodes
        )
    
    Start >> decision_node >> [parallel_branch_nodes, No_Records_in_log_table] 
    
    No_Records_in_log_table >> End
    
    for pn in parallel_nodes:
        
        sql_command = 'db_pgsql_sql_command_'+pn+'_SP'
        sql_cmd = Variable.get(sql_command)
        
        
        pnodes = PostgresOperator(
                task_id = 'call_SPs_'+pn,
                sql = sql_cmd,
                postgres_conn_id = 'db_pgsql_SP_'+pn,
                autocommit = True)
        
        del_pn = PythonOperator(
                task_id = 'del_record_' + pn,
                python_callable = on_success_delete_log_record,
                op_kwargs = {'module_name':"'"+pn+"'"}
                )
        
        parallel_branch_nodes >> pnodes >> del_pn >> End
        
