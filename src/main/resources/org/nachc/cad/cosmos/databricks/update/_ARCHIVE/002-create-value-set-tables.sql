--
-- create the value_set schema (drop if exists)
--

create schema if not exists value_set;

--
-- table for the value sets
--

drop table if exists value_set.value_set;

create table value_set.value_set (
  guid string,
  value_set_name string,
  code_system string,	
  value_set_oid string,	
  type string,	
  definition_version string,	
  steward string,
  purpose_clinical_focus string,	
  purpose_data_element_scope string,	
  purpose_inclusion_criteria string,	
  purpose_exclusion_criteria string,	
  note string
)
using delta;

--
-- table for the value set entries
-- 

drop table if exists value_set.value_set_value;

create table value_set.value_set_value (
  code string,
  description string,
  code_system string,
  code_system_version string,
  code_system_oid string,
  tty string,
  value_set_name string,
  value_set_code_system string,
  value_set_oid string
)
using csv
options (
  header = "true",
  inferSchema = "false",
  path = "/FileStore/tables/prod/global/value-set/csv"
);

--
-- define a group of value sets
--

drop table if exists value_set.value_set_group;

create table value_set.value_set_group (
  guid string,
  name string,
  type string,
  description string,
  created_by string,
  created_date date
)
using delta;

--
-- define what is in the value set group
-- 

drop table if exists value_set.value_set_group_member;

create table value_set.value_set_group_member(
  guid string,
  value_set_group_guid string,
  value_set_oid string,
  value_set_version string, 
  value_set_name string,
  code_system string
)
using delta;


