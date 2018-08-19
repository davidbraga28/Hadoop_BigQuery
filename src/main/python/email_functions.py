# coding=utf-8

###########  Bibliotecas ###############

import pandas as pd

from datetime import datetime
from unicodedata import normalize
from configuration_functions import *

#variaveis global
hive_conn = hive_connect()
hdfs_conn = hdfs_connect()
timestamp = datetime.now().strftime("%H%M%S")
data_atual = get_year_moth_day_now()

###################### FUNCOES GENERICAS ################################

def get_csv_path_now():
    return get_csv_formulario_email_path() + data_atual + "/" + timestamp

def get_history_path():
    return get_path_history() + "/" + data_atual + "/" + timestamp

def move_received_to_process():
    (ret, out, err) = run_cmd(['sudo', '-u', get_user_hdfs(), 'hadoop', 'fs', '-mv',  get_path_received() + '/*', get_path_process()])
    print("File " + get_path_received() + "/*  moved successffuly!" )

def write_hdfs_from_email():
    with hdfs_conn.read((get_path_process() + '/emails.csv')) as email_full:
      email_formated = str(email_full.read().replace('\r','').replace('\n','').replace('\"','#').replace('##','#,#').replace('#,#','|').replace('#','').replace(get_email_header(),''))
      email = remover_acentos(email_formated).replace('.com.br|','.com.br\n').replace(get_email_termo1(),'').replace(get_email_termo2(),'')

    hdfs_conn.write(get_txt_ext_email(),email,overwrite=True)
    
def get_formulario_email():
    hive_conn.execute("SELECT CONCAT(CONCAT(ID,'|'),CONCAT(parent_protocol), TEXT_BODY) FROM EXT.EMAIL")
    item_limpo = ''
    for text_body in hive_conn.fetchall():  
        #Lista valores de formularios do email
        lista_fields=str(text_body).split(": ")
        
        item_parcial = ''
        for item in lista_fields :
          #Valida e Formata os dados para carregar a tabela de formulario email
          item_parcial += str('|' + item.strip().replace('(u','').replace(',)','').replace("'","").replace('SulAmerica - FormularioTipo formulario','').replace('Nome','').replace('Ddd','').replace('Telefone','').replace('E-mail','').replace('Sexo','').replace('UF','').replace('No Cartao Identificacao','').replace('Quem e voce','').replace('Cidade','').replace('Produto','').replace('Mensagem',''))

        item_formatado = item_parcial + '|' + data_atual + '\n'    
        item_limpo += item_formatado [1:]
        
    return item_limpo

def insert_ext_formulario_email():
    print(" Writing HDFS ::: " + get_txt_ext_formulario_email() + " >>>>>>>>>>")
    
    hdfs_conn.write(get_txt_ext_formulario_email(),get_formulario_email().replace("'|","").replace("'",""),overwrite=True)

def insert_work_email():
    query_insert_email = "INSERT INTO TABLE WORK.EMAIL PARTITION ( data_ingestion_partition ) SELECT *,'"+ data_atual +"' data_ingestion, '"+ get_year_moth_day_now() +"' data_ingestion_partition FROM EXT.EMAIL"
    print("<<<<<<< Executing query ::: " + query_insert_email + " >>>>>>>>>>")
    hive_conn.execute("set hive.exec.dynamic.partition.mode=nonstrict")
    hive_conn.execute(query_insert_email)

def insert_work_formulario_email():
    query_insert_formulario_email = "INSERT INTO TABLE WORK.FORMULARIO_EMAIL PARTITION ( data_ingestion_partition ) SELECT *, '"+ data_atual +"' data_ingestion , '"+ get_year_moth_day_now() +"' data_ingestion_partition FROM EXT.FORMULARIO_EMAIL"
    
    print("<<<<<<< Executing query ::: " + query_insert_formulario_email + " >>>>>>>>>>")
    hive_conn.execute(query_insert_formulario_email)

def create_path_now_csv():
    (ret, out, err)= run_cmd(['sudo', '-u', get_user_hdfs(), 'hadoop', 'fs', '-mkdir', '-p', get_csv_path_now()])

def write_csv():
    hdfs_conn.write(str(get_csv_path_now() + "/formulario_emails.csv"),get_formulario_email().replace("'|","").replace('|',','),overwrite=True)
    
def move_process_to_history():
    print("<<<<<<<<< Moving for history... >>>>>>>>>>")
    (ret, out, err)= run_cmd(['sudo', '-u', get_user_hdfs(), 'hadoop', 'fs', '-mkdir', '-p', get_history_path()])
    
    (ret, out, err)= run_cmd(['sudo', '-u', get_user_hdfs(), 'hadoop', 'fs', '-cp', get_path_process() + '/*', get_history_path()])    
