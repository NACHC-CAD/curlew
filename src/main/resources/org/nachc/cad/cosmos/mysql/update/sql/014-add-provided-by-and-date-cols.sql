use cosmos;

alter table raw_table_file
add provided_by varchar(256);

alter table raw_table_file
add provided_date datetime;

