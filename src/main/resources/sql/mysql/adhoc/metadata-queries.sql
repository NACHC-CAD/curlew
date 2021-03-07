-- * * *
--
-- METADATA-QUERIES 
--
-- * * *

use cosmos;

select * from org_code
order by code;

-- 
-- get all the entities for a project
-- 

select distinct group_table_name
from raw_table_group
where project = 'ai'
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
order by org_code, group_table_name, file_name
;

--
-- get all the files for a project and org
--

select 
	fil.org_code, grp.group_table_name, fil.file_name, fil.file_size, fil.file_size_units
from 
	raw_table_file fil
    join raw_table tab on tab.guid = fil.raw_table 
    join raw_table_group grp on tab.raw_table_group = grp.guid
where 1=1
	and fil.project = 'covid'
    and fil.org_code = 'ac'
order by org_code, group_table_name, file_name
;

--
-- get all the orgs for a project
--

select distinct org_code
from raw_table_col_detail
where project = 'hiv'
order by org_code
;

--
-- get all the columns for a project and org
--

select col_index as col, file_name, project, org_code, group_table_name, col_name, col_alias
from raw_table_col_detail
where project = 'hiv'
and org_code = 'oachc'
order by project, group_table_name, col, col_alias
;

--
-- get all the columns for a project
--

select col_index as col, file_name, project, org_code, group_table_name, col_name, col_alias
from raw_table_col_detail
where project = 'ai'
order by project, group_table_name, col, col_alias
;

--
-- get all the column aliases for a project
--

select col_index as col, file_name, project, org_code, group_table_name, col_name, col_alias
from raw_table_col_detail
where project = 'covid'
and col_alias is not null
order by project, group_table_name, org_code, col, col_alias
;

-- 
-- show distinct file names
--

select file_name, count(*) from raw_table_file
group by 1
order by 2 desc
;

select * from raw_table_file;

select * from raw_table_col;

