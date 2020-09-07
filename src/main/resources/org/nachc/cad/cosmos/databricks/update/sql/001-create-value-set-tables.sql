--
-- create the value_set schema (drop if exists)
--

create schema value_set;

--
-- table for the value sets
--

create table value_set.value_set (
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
);

--
-- table for the value set entries
-- 

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
);

--
-- define a group of value sets
--

create table value_set.value_set_group (
  guid string,
  name string,
  type string,
  description string,
  created_by string,
  created_date date
);

--
-- define what is in the value set group
-- 

create table value_set.value_set_group_member (
  value_set_group_guid string,
  value_set_oid string,
  value_set_definition_version string
);




