# coding=utf-8

###########  Bibliotecas ###############

import os
import csv

from google.cloud import bigquery
from google.cloud.bigquery.client import Client

###########  Bibliotecas ###############

#import commons functions
from configuration_functions import *

####################################################################

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = get_google_credentials()
client = Client()

hive_conn = hive_connect()

def get_name_database_bigquery():
    return "hive"

def get_name_table_bigquery():
    return "email_hadoop"

def get_schema_email_bigquery():
    return [
        bigquery.SchemaField('id_email', 'STRING', mode='required'),
        bigquery.SchemaField('parent_protocol_email', 'STRING', mode='required'),
        bigquery.SchemaField('tipo', 'STRING', mode='required'),
        bigquery.SchemaField('from_email', 'STRING', mode='nullable'),
        bigquery.SchemaField('mensagem', 'STRING', mode='nullable')
    ]

def instance_database_bigquery(name_database):
    dataset_ref = client.dataset(name_database)
    dataset = bigquery.Dataset(dataset_ref)
    return dataset

def instance_table_bigquery(name_database, name_table):
    dataset = instance_database_bigquery(name_database)
    table_ref = dataset.table(name_table)
    table = bigquery.Table(table_ref, schema=get_schema_email_bigquery())
    
    return table

def create_database_bigquery(name_database):
    dataset = instance_database_bigquery(name_database)
    dataset = client.create_dataset(dataset)
    
def create_table_bigquery(name_database, name_table):
    dataset = instance_database_bigquery(name_database)
      
    table_ref = dataset.table(name_table)
    table = bigquery.Table(table_ref, schema=get_schema_email_bigquery())
    table = client.create_table(table)      
    assert table.table_id == name_table
    
def insert_table_from_file_bigquery(tupla, table_ref, type_file, skip_header):
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = table_ref
    job_config.skip_leading_rows = skip_header
    job_config.autodetect = True
    job = client.load_table_from_file(tupla, table_ref, job_config=job_config)  # API request

def insert_rows_table_bigquery(table_name, array_tuples):
    try:
      errors = client.insert_rows(table_name, array_tuples)  # API request
    except:
      print('Não salvou!')
      
def insert_informations_email_bigquery():
  
    rows_insert = []  
    hive_conn.execute("""
    SELECT 
      CASE WHEN id_email is null THEN '' ELSE id_email END,
      CASE WHEN parent_protocol_email is null THEN '' ELSE parent_protocol_email END,
      CASE WHEN tipo is null THEN '' ELSE tipo END,
      CASE WHEN email is null THEN '' ELSE email END,
      CASE WHEN mensagem is null THEN '' ELSE mensagem END 
    FROM WORK.FORMULARIO_EMAIL
    """)
    
    for row in hive_conn.fetchall():  
        rows_insert.append(tuple(row))
        
    insert_rows_table_bigquery(instance_table_bigquery(get_name_database_bigquery(), get_name_table_bigquery()), rows_insert)
