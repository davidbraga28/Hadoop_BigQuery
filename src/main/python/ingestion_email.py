# coding=utf-8

###########  Bibliotecas ###############

#import commons functions
from configuration_functions import *
#import email functions
from email_functions import *
#import bigquery functions
from bigquery_functions import *

####################################################################

print("###################### Start ingestion Email ... ############################")

#Move arquivos recebidos para processar
move_received_to_process()

#Ler arquivo recebido, formata e escreve no hdfs da tabela ext.email
write_hdfs_from_email()

#Escreve arquivo na tabela ext.formulario_email
insert_ext_formulario_email()

#Insere na tebela gerenciavel work.email
insert_work_email()

#Insere na tebela gerenciavel work.formulario_email
insert_work_formulario_email()

#Cria diretorio com timestamp atual para o processo 
create_path_now_csv()

#Escreve arquivo csv no HDFS
write_csv()

############# Move arquivos processados para o historico ###############
move_process_to_history()

insert_informations_email_bigquery()

print("################################# End ingestion Email ########################################")

