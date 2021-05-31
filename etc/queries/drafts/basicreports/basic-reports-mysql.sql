-- * * *
--
-- BASIC REPORT QUERIES
--
-- * * *

use cosmos;

--
-- files for project
--

select distinct
    org_code org,
    data_lot "Data Lot",
    group_table_name "Data Group",
	file_name "File Name",
    format(file_size,0) "File Size (b)",
    provided_by "Provided By",
    provided_date "Provided Date"
from
	raw_table_col_detail
where 1=1
	and project = 'covid'
--    and org_code in ('ochin','ac')
order by 1,2,3
;

show tables;

select * from org_code order by code;

select distinct org_code, group_table_name, col_name 
from raw_table_col_detail
where 1=1
--	and (lower(col_name) like '%vacc%' or lower(group_table_name) like '%flat%')
    and project = 'covid'
    and org_code = 'hcn'
order by group_table_name, col_name
;

 
select * from raw_table_col_detail
where col_name = 'vaccine_group'
;