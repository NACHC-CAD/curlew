create schema cosmos;

create table cosmos.build_version (
  version_number string,
  file_name string
);

insert into cosmos.build_version values ('000', '000-create-schema.sql');
