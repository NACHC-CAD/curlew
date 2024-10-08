use cosmos;

--
-- get distinct column names for demo
--

select distinct
	grp.name,
	grp.code,
    coalesce(col.col_alias, col.col_name) col_name
from
	raw_table_group grp
    join raw_table tbl on tbl.raw_table_group = grp.guid
    join raw_table_file fil on fil.raw_table = tbl.guid
    join raw_table_col col on col.raw_table = tbl.guid
where
	grp.code = 'womens_health_demo'
order by col_name, org_code
;

--
-- get column names for demo
--

select
	grp.name,
	grp.code,
    col.dirty_name,
    tbl.raw_table_schema,
    tbl.raw_table_name,
    fil.org_code,
    col.col_name,
    col.col_alias
from
	raw_table_group grp
    join raw_table tbl on tbl.raw_table_group = grp.guid
    join raw_table_file fil on fil.raw_table = tbl.guid
    join raw_table_col col on col.raw_table = tbl.guid
where
	grp.code = 'womens_health_demo'
order by col_name, org_code
;

--
-- get column names for enc
--

select 
	grp.name,
	grp.code,
    col.dirty_name,
    tbl.raw_table_schema,
    tbl.raw_table_name,
    fil.org_code,
    col.col_name,
    col.col_alias
from
	raw_table_group grp
    join raw_table tbl on tbl.raw_table_group = grp.guid
    join raw_table_file fil on fil.raw_table = tbl.guid
    join raw_table_col col on col.raw_table = tbl.guid
where
	grp.code = 'womens_health_enc'
order by col_name, org_code
;

--
-- get distinct column names for demo
--

select distinct
	grp.name,
	grp.code,
    coalesce(col.col_alias, col.col_name) col_name
from
	raw_table_group grp
    join raw_table tbl on tbl.raw_table_group = grp.guid
    join raw_table_file fil on fil.raw_table = tbl.guid
    join raw_table_col col on col.raw_table = tbl.guid
where
	grp.code = 'womens_health_enc'
order by col_name, org_code
;


--
-- column names for demo
-- 

select
	grp.name,
	grp.code,
    tbl.raw_table_name,
    col.dirty_name,
    fil.org_code,
    col.col_name,
    col.col_alias
from
	raw_table_group grp
    join raw_table tbl on tbl.raw_table_group = grp.guid
    join raw_table_file fil on fil.raw_table = tbl.guid
    join raw_table_col col on col.raw_table = tbl.guid
where 1=1
	and grp.code like 'womens_health_dx'
order by org_code, col_name
;
	
--
-- column names for enc
-- 

select
	grp.name,
	grp.code,
    col.dirty_name,
    fil.org_code,
    col.col_name,
    col.col_alias
from
	raw_table_group grp
    join raw_table tbl on tbl.raw_table_group = grp.guid
    join raw_table_file fil on fil.raw_table = tbl.guid
    join raw_table_col col on col.raw_table = tbl.guid
where 1=1
	and grp.code like 'womens_health%'
    and col.col_alias is not null
order by code, col_name, org_code
;
	

select * from raw_table_col where col_alias is not null order by col_name;

