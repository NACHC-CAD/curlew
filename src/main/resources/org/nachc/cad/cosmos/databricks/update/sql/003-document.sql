--
-- table to track information about documents
--

create table cosmos.document (
  guid string,
  document_name string,
  document_size double,
  document_size_unit string,
  document_type string,
  document_use_type string,
  databricks_path string,
  parent_guid string,
  parent_rel_type string,
  created_by string,
  created_date date
);

