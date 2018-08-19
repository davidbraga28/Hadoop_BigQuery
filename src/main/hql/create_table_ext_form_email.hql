drop table ext.formulario_email;
CREATE EXTERNAL TABLE ext.formulario_email
(
id_email string,
parent_protocol_email string,
tipo string,
nome string,
ddd string,
telefone string,
email string,
sexo string,
uf string,
quem_e_voce string,
cidade string,
produto string,
mensagem string,
id_cartao string
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/atendimento/call_center/email/tabelas/formulario_email/';
