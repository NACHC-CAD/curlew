--
-- query to get the distinct orgs
--

select distinct
    file.proj_code,
	org_code as org_name
from
	raw_table_group grp
	join raw_table tbl on tbl.raw_table_group = grp.guid
    join raw_table_file file on file.raw_table = tbl.guid
where
	file.proj_code = 'womens_health'
;


--
-- query to get the raw data tables
-- 

select
    grp.group_table_name table_nam,
    file.org_code org_name,
    file.file_name file_name,
    file.file_size file_size,
    file.file_size_units file_size_units
from
	raw_table_group grp
	join raw_table tab on tab.raw_table_group = grp.guid
    join raw_table_file file on file.raw_table = tab.guid
order by 1,2,3,4
;



    
