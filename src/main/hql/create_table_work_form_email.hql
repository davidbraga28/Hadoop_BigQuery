drop table work.formulario_email;
CREATE TABLE IF NOT EXISTS work.formulario_email 
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
id_cartao string,
data_ingestion string
)
PARTITIONED BY ( data_ingestion_partition string )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS ORC
TBLPROPERTIES
(
'serialization.null.format'='', 
'orc.compress'='SNAPPY',
'orc.bloom.filter.columns'='id_email'
);
