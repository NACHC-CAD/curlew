--
-- script to create initial views for cosmos
-- 

use cosmos;

create or replace view cosmos.raw_table_col_detail as (
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
    fil.org_code,
    fil.data_lot,
    fil.provided_by,
    fil.provided_date,
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

create or replace view raw_table_detail as (
select distinct
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
    fil.org_code,
    fil.data_lot
from 
	raw_table_group grp
	join raw_table tbl on tbl.raw_table_group = grp.guid
	join raw_table_file fil on fil.raw_table = tbl.guid
);

create or replace view table_col_summary as
select distinct
	project,
	org_code,
    group_table_name,
    dirty_name,
    col_name,
    col_alias,
    group_concat(distinct file_name order by file_name SEPARATOR',') files
from
	raw_table_col_detail
group by 1,2,3,4,5,6
;

create or replace view table_file_summary as
select distinct
	project,
    org_code,
    data_lot,
    group_table_name,
    provided_by,
    provided_date,
    file_name,
    file_size
from
	raw_table_col_detail
order by 1,2,3,4,7
;

create or replace view table_file_by_lot as
select
	project,
    org_code,
    data_lot,
    min(provided_date) first_submission,
    max(provided_date) last_submision,
    sum(file_size) total_bytes,
    group_concat(distinct provided_by) provided_by_set,
    count(distinct file_name) number_of_files
from
	table_file_summary
group by 1,2,3
order by 1,2,3
;
