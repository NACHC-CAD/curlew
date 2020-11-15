--
-- script to create initial views for cosmos
-- 

use cosmos;

create view cosmos.raw_table_col_detail as (
select
	tbl.guid raw_table,
	grp.guid raw_table_group,
    fil.guid raw_table_file,
    col.guid raw_table_col,
    grp.project project,
    grp.code raw_table_group_code,
    grp.name raw_table_group_name,
    grp.description raw_table_group_desc,
    grp.file_location group_file_location,
    grp.raw_table_schema group_raw_table_schema,
    grp.group_table_schema,
    grp.group_table_name,
    tbl.raw_table_schema,
    tbl.raw_table_name,
    fil.file_location,
    fil.file_name,
    fil.file_size,
    fil.file_size_units,
    col.col_index,
    col.dirty_name,
    col.col_name,
    col.col_alias,
    col.real_name
from
	raw_table tbl
    join raw_table_col col on col.raw_table = tbl.guid
    join raw_table_file fil on fil.raw_table = tbl.guid
    join raw_table_group grp on tbl.raw_table_group = grp.guid
);


create view raw_table_col_alias as (
select
    grp.code group_code,
    tbl.raw_table_schema,
    tbl.raw_table_name,
    col.col_name,
    col.col_alias,
	tbl.guid raw_table,
	grp.guid raw_table_group,
    fil.guid raw_table_file,
    col.guid raw_table_col
from
	raw_table tbl
    join raw_table_col col on col.raw_table = tbl.guid
    join raw_table_file fil on fil.raw_table = tbl.guid
    join raw_table_group grp on tbl.raw_table_group = grp.guid
);

create view raw_table_detail as (
select 
	grp.guid raw_table_group_guid,
    grp.project,
    grp.code group_code,
    grp.name group_name,
    grp.description group_description,
    grp.file_location group_file_location,
    grp.group_table_name,
    grp.group_table_schema,
	tbl.guid raw_table_guid,
    tbl.raw_table_name,
    tbl.raw_table_schema,
    fil.file_location,
    fil.file_name,
    fil.file_size,
    fil.file_size_units,
    fil.org_code
from 
	raw_table_group grp
	join raw_table tbl on tbl.raw_table_group = grp.guid
	join raw_table_file fil on fil.raw_table = tbl.guid
)
;
