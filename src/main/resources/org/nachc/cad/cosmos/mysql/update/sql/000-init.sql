drop schema if exists cosmos;

create schema cosmos;

use cosmos;

create table person (
	guid varchar(40),
    username varchar(64),
    fname varchar(64),
    lname varchar(64),
    display_name varchar(256),
    created_by varchar(40),
    created_date date,
    updated_by varchar(40),
    updated_date date,
    unique (username),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table file_type (
	guid varchar(40),
    code varchar(64),
    name varchar(64),
    description varchar(256),
    created_by varchar(40),
    created_date date,
    updated_by varchar(40),
    updated_date date,
    unique (code),
    unique (name),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table document_role (
	guid varchar(40),
    code varchar(64),
    name varchar(65),
    description varchar(256),
    created_by varchar(40),
    created_date date,
    updated_by varchar(40),
    updated_date date,
    unique (code),
    unique (name),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table status (
	guid varchar(40),
    code varchar(64),
    name varchar(64),
    description varchar(256),
    created_by varchar(40),
    created_date date,
    updated_by varchar(40),
    updated_date date,
    unique (code),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table project (
	guid varchar(40),
    code varchar(256),
    name varchar(256),
    description varchar(1024),
    created_by varchar(40),
    created_date date,
    updated_by varchar(40),
    updated_date date,
    unique (code),
    unique (name),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table document_validator (
	guid varchar(40),
    name varchar(256),
    description varchar(1024),
    class_name varchar(256),
    created_by varchar(40),
    created_date date,
    updated_by varchar(40),
    updated_date date,
    unique(name),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table block_def (
	guid varchar(40),
    name varchar(256),
    title varchar(256),
    description varchar(1024),
    project varchar(40),
    created_by varchar(40),
    created_date date,
    updated_by varchar(40),
    updated_date date,
    unique (name),
    unique (title),
    foreign key (project) references project (guid),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table document_def (
	guid varchar(40),
    block_def varchar(40),
    file_type varchar(40),
    document_role varchar(40),
    row_id int,
    name varchar(256),
    description varchar(1024),
    validator varchar(40),
    databricks_dir varchar(256),
    created_by varchar(40),
    created_date date,
    updated_by varchar(40),
    updated_date date,
    foreign key (block_def) references block_def (guid),
    foreign key (file_type) references file_type (guid),
    foreign key (document_role) references document_role (guid),
    foreign key (validator) references document_validator (guid),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table block (
	guid varchar(40),
    title varchar(64),
    description varchar(1024),
    block_def varchar(40),
    status varchar(40),
    created_by varchar(40),
    created_date date,
    updated_by varchar(40),
    updated_date date,
    foreign key (block_def) references block_def (guid),
    foreign key (status) references status (guid),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

create table document (
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
    created_date date,
    updated_by varchar(40),
    updated_date date,
    foreign key (block) references block (guid),
	foreign key (file_type) references file_type (guid),
    foreign key (document_role) references document_role (guid),
    foreign key (document_def) references document_def (guid),
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid),
    primary key (guid)
);

