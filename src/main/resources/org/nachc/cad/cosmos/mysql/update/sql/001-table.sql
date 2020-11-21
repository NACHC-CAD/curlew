-- 
-- These tables exist only to manage raw data files.  
-- Higher level tables and table relationships will be delt with elsewhere
--

use cosmos;

create table proj_url (
	guid varchar(40),
	project varchar(64) not null,
    sort_order int,
    url varchar(256) not null,
    url_type varchar(256) not null,
    link_text varchar(256) not null,
    url_description varchar(256),
    primary key (guid),
    foreign key (project) references proj_code(code),
	created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid)
);

create table cosmos.raw_table_group (
	guid varchar(40),
    project varchar(64) not null,
	code varchar(256),
	name varchar(256),
	description varchar(256),
	file_location varchar(256),
	raw_table_schema varchar(256),
	group_table_schema varchar(256),
	group_table_name varchar(256),
	primary key(guid),
	unique(code),
	unique(name),
	unique(description),
	unique(raw_table_schema),
	unique(group_table_schema),
    unique(file_location),
    foreign key (project) references proj_code (code),
	created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid)
);

create table cosmos.raw_table (
	guid varchar(40),
    project varchar(64) not null,
	raw_table_schema varchar(256),
	raw_table_name varchar(256),
    raw_table_group varchar(256),
	primary key (guid),
    foreign key (raw_table_group) references raw_table_group (guid),
    foreign key (project) references  proj_code (code),
	unique (raw_table_schema, raw_table_name),
	created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid)
);

create table cosmos.raw_table_file (
	guid varchar(40),
    project varchar(64) not null,
    data_lot varchar(64) not null,
	raw_table varchar(40),
	file_location varchar(256),
	file_name varchar(256),
    file_size long,
    file_size_units varchar(8),
    org_code varchar(64),
    unique (raw_table),
    unique (file_location, file_name),
    foreign key(org_code) references org_code (code),
    foreign key(project) references proj_code (code),
	created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid)
);

create table cosmos.raw_table_col (
	guid varchar(40),
    project varchar(64) not null,
	raw_table varchar(256),
    col_index int,
	dirty_name varchar(256),
	col_name varchar(256),
	col_alias varchar(256),
    real_name varchar(256),
	primary key (guid),
	unique (raw_table, col_name),
	foreign key (raw_table) references raw_table(guid),
    foreign key (project) references  proj_code (code),
	created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid)
);
