-- * * *
--
-- METADATA-QUERIES 
--
-- * * *

-- 
-- get all the entities for a project
-- 

select distinct group_table_name
from raw_table_group
where project = 'covid'
order by group_table_name;

--
-- get all the files for a project
--

select 
	fil.org_code, grp.group_table_name, fil.file_name, fil.file_size, fil.file_size_units
from 
	raw_table_file fil
    join raw_table tab on tab.guid = fil.raw_table 
    join raw_table_group grp on tab.raw_table_group = grp.guid
where 
	fil.project = 'covid'
order by org_code, file_name
;

--
-- get all the columns for a table
--

select raw_table_col, project, org_code, group_table_name, col_name, col_alias
from raw_table_col_detail
where project = 'covid'
order by project, group_table_name, col_name, col_alias
;
