drop schema if exists cosmos;

create schema cosmos;

use cosmos;

create table cosmos.person (
	guid varchar(40),
    username varchar(64),
    fname varchar(64),
    lname varchar(64),
    display_name varchar(256),
    password varchar(256),
    salt varchar(40),
    created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    unique (username),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table cosmos.file_type (
    code varchar(64),
    name varchar(64),
    description varchar(256),
    created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    unique (name),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (code)
);

create table cosmos.document_role (
    code varchar(64),
    name varchar(65),
    description varchar(256),
    created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    unique (name),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (code)
);

create table cosmos.status (
    code varchar(64),
    name varchar(64),
    description varchar(256),
    created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (code)
);

create table cosmos.project (
	guid varchar(40),
    code varchar(256),
    name varchar(256),
    description varchar(1024),
    created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    unique (code),
    unique (name),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table cosmos.document_validator (
	guid varchar(40),
    name varchar(256),
    description varchar(1024),
    class_name varchar(256),
    created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    unique(name),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table cosmos.block_def (
	guid varchar(40),
    code varchar(256),
    name varchar(256),
    description varchar(1024),
    project varchar(40),
    created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    unique (code),
    unique (name),
    foreign key (project) references project (guid),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table cosmos.document_def (
	guid varchar(40),
    row_id int,
    block_def varchar(40),
    file_type varchar(40),
    document_role varchar(40),
    data_group varchar(64),
    name varchar(256),
    description varchar(1024),
    validator varchar(40),
    databricks_dir varchar(256),
    created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    foreign key (block_def) references block_def (guid),
    foreign key (file_type) references file_type (code),
    foreign key (document_role) references document_role (code),
    foreign key (validator) references document_validator (guid),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table cosmos.block (
	guid varchar(40),
    title varchar(64),
    description varchar(1024),
    block_def varchar(40),
    status varchar(40),
    created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    foreign key (block_def) references block_def (guid),
    foreign key (status) references status (code),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table cosmos.document (
	guid varchar(40),
    block varchar(40),
    file_name varchar(256),
    file_description varchar(256),
    file_type varchar(40),
    document_role varchar(40),
    databricks_dir varchar(256),
    document_def varchar(40),
    databricks_file_name varchar(256),
    created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    foreign key (block) references block (guid),
	foreign key (file_type) references file_type (code),
    foreign key (document_role) references document_role (code),
    foreign key (document_def) references document_def (guid),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table cosmos.build_version (
  version_number varchar(8),
  file_name varchar(256)
);

insert into cosmos.build_version values ('000', '000-create-schema.sql');
