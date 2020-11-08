select * from raw_table_group;

select 
	*
from 
	raw_table tbl
    join raw_table_file fil on fil.raw_table = tbl.guid
    join raw_table_group grp on tbl.raw_table_group = grp.guid
;

--
-- query for all files
-- 

select 
    grp.code as "table",
	fil.org_code org,
    fil.file_name file,
    fil.file_size file_size,
    fil.file_size_units units
from 
	raw_table tbl
    join raw_table_file fil on fil.raw_table = tbl.guid
    join raw_table_group grp on tbl.raw_table_group = grp.guid
order by code, org_code, file_name
;

--
-- query for data tables for all files
-- 

select 
    grp.code as "table",
	fil.org_code org,
    fil.file_name file,
    fil.file_size file_size,
    fil.file_size_units units,
    tbl.raw_table_schema,
    tbl.raw_table_name
from 
	raw_table tbl
    join raw_table_file fil on fil.raw_table = tbl.guid
    join raw_table_group grp on tbl.raw_table_group = grp.guid
;
