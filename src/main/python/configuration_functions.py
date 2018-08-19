# coding=utf-8

###########  Bibliotecas ###############

import subprocess
import pandas as pd
import snappy

from datetime import datetime
from unicodedata import normalize
from hdfs import InsecureClient
from pyhive import hive


############## Funcoes ###############

def get_google_credentials():
    return "/home/david_braga/service.json"
    
def get_user_hive():
    return "hive"
    
def get_user_hdfs():
    return "hdfs"
    
def get_hive_host():
    return "hdp02"

def get_hdfs_host():
    return "http://hdp01:50070"

def get_beeline_connect():
    return "jdbc:hive2://hdp02:10000/"

def get_year_moth_day_now():
    now = datetime.now()

    mes = ""
    if len(str(now.month)) > 1: 
      mes=str(now.month) 
    else: 
      mes=str("0" + str(now.month))
    
    day = ""
    if len(str(now.day)) > 1: 
      day=str(now.day) 
    else: 
      day=str("0" + str(now.day))
    
    return str(str(now.year) + mes + day)
    
def snappy_compress(path):
        path_to_store = path+'.snappy'

        with open(path, 'rb') as in_file:
          with open(path_to_store, 'w') as out_file:
            snappy.stream_compress(in_file, out_file)
            out_file.close()
            in_file.close()

        return path_to_store

def run_cmd(args_list):
         """
         run linux commands
         """
         # import subprocess
         print('Running system command: {0}'.format(' '.join(args_list)))

         print args_list
         proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

         s_output, s_err = proc.communicate()

         s_return =  proc.returncode
         return s_return, s_output, s_err  

#Funcao para remover acentos e formatar encode UTF-8
def remover_acentos(txt, codif='utf-8'):
  return normalize('NFKD', txt.decode(codif)).encode('ASCII','ignore')

def hive_connect():
  conn = hive.Connection(host=get_hive_host(), port=10000, username=get_user_hive())
  return conn.cursor()         

def hdfs_connect():
    return InsecureClient(get_hdfs_host(), user=get_user_hdfs())

###################### CONFIGURACOES GENERICAS ################################

def get_name_area():
    return "/atendimento"

def get_name_project():
    return "/call_center"

def get_name_sub_project():
    return "/email"

def get_hdfs_path_main():
    return get_name_area() + get_name_project() + get_name_sub_project() 

def get_local_path_deploy():
    return get_name_area() + get_name_project() + get_name_sub_project()

def get_txt_ext_email():
    return get_hdfs_path_main() + "/tabelas/email/emails.txt"

def get_txt_ext_formulario_email():
    return get_hdfs_path_main() + "/tabelas/formulario_email/formulario_emails.txt"

def get_csv_formulario_email_path():
    return get_hdfs_path_main() + "/csv/formulario_email/"

### Estrutura hdfs email

def get_path_received():
    return get_hdfs_path_main() + "/received"

def get_path_process():
    return get_hdfs_path_main() + "/process"

def get_path_history():
    return get_hdfs_path_main() + "/history"

def get_path_csv():
    return get_hdfs_path_main() + "/csv"

### Estrutura deploy email

def get_deploy_name_project():
    return "/cadastro_email-0.0.1"

def get_deploy_sh():
    return get_hdfs_path_main() + get_deploy_name_project + "/sh"

def get_deploy_java():
    return get_hdfs_path_main() + get_deploy_name_project + "/java"

def get_deploy_python():
    return get_hdfs_path_main() + get_deploy_name_project + "/python"

def get_deploy_hql():
    return get_hdfs_path_main() + get_deploy_name_project + "/hql"


#####################################################################################
