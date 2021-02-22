-- * * *
--
-- METADATA-QUERIES 
--
-- * * *

use cosmos;

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
-- get all the columns for a project and org
--

select col_index as col, file_name, project, org_code, group_table_name, col_name, col_alias
from raw_table_col_detail
where project = 'covid'
and org_code = 'chcn'
order by project, group_table_name, col, col_alias
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

