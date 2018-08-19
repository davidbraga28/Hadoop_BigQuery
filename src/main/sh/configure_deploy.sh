#!/bin/bash

sudo -u hdfs hadoop fs -mkdir -p /atendimento
sudo -u hdfs hadoop fs -chown david_braga:david_braga /atendimento

sudo -u david_braga hadoop fs -mkdir -p /atendimento/call_center/email/received/
sudo -u david_braga hadoop fs -mkdir -p /atendimento/call_center/email/process/
sudo -u david_braga hadoop fs -mkdir -p /atendimento/call_center/email/history/
sudo -u david_braga hadoop fs -mkdir -p /atendimento/call_center/email/csv/

beeline -u jdbc:hive2://hdp02:10000/ -n hive -e "create schema ext;"
beeline -u jdbc:hive2://hdp02:10000/ -n hive -e "create schema work;" 

beeline -u jdbc:hive2://hdp02:10000/ -n david_braga -f /home/david_braga/sulamerica_call_center_email-0.0.1/hql/create_table_ext_email.hql
beeline -u jdbc:hive2://hdp02:10000/ -n david_braga -f /home/david_braga/sulamerica_call_center_email-0.0.1/hql/create_table_ext_form_email.hql

beeline -u jdbc:hive2://hdp02:10000/ -n hive -f /home/david_braga/sulamerica_call_center_email-0.0.1/hql/create_table_work_email.hql
beeline -u jdbc:hive2://hdp02:10000/ -n hive -f /home/david_braga/sulamerica_call_center_email-0.0.1/hql/create_table_work_form_email.hql

