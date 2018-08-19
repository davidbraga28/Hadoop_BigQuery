drop table work.email;
CREATE TABLE work.email
(
id string,
parent_protocolo string,
subject string,
text_body string,
to_address string,
data_ingestion string
)
PARTITIONED BY ( data_ingestion_partition string )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS ORC
TBLPROPERTIES
(
'serialization.null.format'='', 
'orc.bloom.filter.columns'='id'
);
