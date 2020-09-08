
-- 
-- table to track value_set files
-- 

drop table if exists value_set.value_set_file;

create table value_set.value_set_file (
  file_guid string,
  file_location string,
  file_parent string,
  created_by string,
  created_date date
);
