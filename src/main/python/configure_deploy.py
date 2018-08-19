# coding=utf-8

###########  Bibliotecas ###############

from configuration_functions import *
from bigquery_functions import *

print(get_beeline_connect())

def deploy_project():
  #AQUI CONFIGURAR ABSOLUTE PATH DO DEPLOY
	hql_path_atual =  str('/home/david_braga/cadastro_email-0.0.1/hql/')
	(ret, out, err)= run_cmd(['sudo', '-u', get_user_hdfs(), 'hadoop', 'fs', '-mkdir', '-p', '/atendimento'])
	(ret, out, err)= run_cmd(['sudo', '-u', get_user_hdfs(), 'hadoop', 'fs', '-mkdir', '-p', '/atendimento/call_center/email/received/'])
	(ret, out, err)= run_cmd(['sudo', '-u', get_user_hdfs(), 'hadoop', 'fs', '-mkdir', '-p', '/atendimento/call_center/email/process/'])
	(ret, out, err)= run_cmd(['sudo', '-u', get_user_hdfs(), 'hadoop', 'fs', '-mkdir', '-p', '/atendimento/call_center/email/history/'])
	(ret, out, err)= run_cmd(['sudo', '-u', get_user_hdfs(), 'hadoop', 'fs', '-mkdir', '-p', '/atendimento/call_center/email/csv/'])
	
	(ret, out, err)= run_cmd(['beeline', '-u', get_beeline_connect(), '-n', get_user_hive(), '-e', 'create schema work'])
	(ret, out, err)= run_cmd(['beeline', '-u', get_beeline_connect(), '-n', get_user_hive(), '-e', 'create schema ext'])
 
	(ret, out, err)= run_cmd(['beeline', '-u', get_beeline_connect(), '-n', get_user_hdfs(), '-f', hql_path_atual + 'create_table_ext_email.hql'])
	(ret, out, err)= run_cmd(['beeline', '-u', get_beeline_connect(), '-n', get_user_hdfs(), '-f', hql_path_atual + 'create_table_ext_form_email.hql'])	
 
	(ret, out, err)= run_cmd(['beeline', '-u', get_beeline_connect(), '-n', get_user_hive(), '-f', hql_path_atual + 'create_table_work_email.hql'])
	(ret, out, err)= run_cmd(['beeline', '-u', get_beeline_connect(), '-n', get_user_hive(), '-f', hql_path_atual + 'create_table_work_form_email.hql'])
     
     
deploy_project()
	
#Cria database e tabelas bigquery
create_database_bigquery(get_name_database_bigquery())
create_table_bigquery(get_name_database_bigquery(), get_name_table_bigquery())    
	
