drop table ext.email;
CREATE EXTERNAL TABLE ext.email
(
id string,
parent_protocolo string,
subject string,
text_body string,
to_address string
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/atendimento/call_center/email/tabelas/email/';
