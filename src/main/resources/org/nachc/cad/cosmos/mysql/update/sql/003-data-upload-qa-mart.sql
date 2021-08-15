--
-- script to create data mart for data upload qa
-- 

drop database if exists cosmos_data_upload_qa;

create database if not exists cosmos_data_upload_qa;

use cosmos_data_upload_qa;

create or replace view org as 
select distinct 
	org_code as org
from cosmos.raw_table_col_detail
;

create or replace view data_lot as
select distinct
	project,
    org_code as org,
    data_lot,
    provided_by,
    provided_date
from
	cosmos.raw_table_col_detail
;

create or replace view file as
select distinct
	project,
    org_code as org,
	raw_table_group_code as table_name,
    file_name,
    data_lot,
    provided_by,
    provided_date
from
	cosmos.raw_table_col_detail
;
	


