-- 
-- These tables exist only to manage raw data files.  
-- Higher level tables and table relationships will be delt with elsewhere
--

drop table if exists cosmos.raw_table_group_col;
drop table if exists cosmos.raw_table_group_raw_table;
drop table if exists cosmos.raw_table_col;
drop table if exists cosmos.raw_table_file;
drop table if exists cosmos.raw_table;
drop table if exists cosmos.raw_table_group;

create table org_code (
	code varchar(64),
    name varchar(256),
    primary key (code),
    unique(name)
);

insert into org_code values ('ac', 'Alliance Chicago');
insert into org_code values ('denver', 'Denver Health');
insert into org_code values ('ochin', 'Oregon Community Health Information Network');

create table proj_code (
	code varchar(64),
    name varchar(256),
    primary key (code),
    unique(name)
);

insert into proj_code values ('womens_health', 'Women''s Health');

create table cosmos.raw_table_group (
	guid varchar(40),
	code varchar(256),
	name varchar(256),
	description varchar(256),
	raw_table_schema varchar(256),
	group_table_schema varchar(256),
	group_table_name varchar(256),
	primary key(guid),
	unique(code),
	unique(name),
	unique(description),
	unique(raw_table_schema),
	unique(group_table_schema),
	created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid)
);

create table cosmos.raw_table (
	guid varchar(40),
	raw_table_schema varchar(256),
	raw_table_name varchar(256),
	primary key (guid),
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
	raw_table varchar(40),
	file_location varchar(256),
	file_name varchar(256),
    file_size long,
    file_size_units varchar(8),
    org_code varchar(64),
    proj_code varchar(64),
    unique (raw_table),
    unique (file_location, file_name),
    foreign key(org_code) references org_code(code),
    foreign key(proj_code) references proj_code(code),
	created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid)
);

create table cosmos.raw_table_col (
	guid varchar(40),
	raw_table varchar(256),
    col_index int,
	dirty_name varchar(256),
	col_name varchar(256),
	col_alias varchar(256),
    real_name varchar(256),
	primary key (guid),
	unique (raw_table, col_name),
	foreign key (raw_table) references raw_table(guid),
	created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid)
);

create table cosmos.raw_table_group_raw_table (
	guid varchar(40),
	raw_table_group varchar(40),
	raw_table varchar(40),
	primary key (guid),
	foreign key (raw_table_group) references raw_table_group (guid),
	foreign key (raw_table) references raw_table(guid),
	unique (raw_table_group, raw_table),
	created_by varchar(40),
    created_date datetime,
    updated_by varchar(40),
    updated_date datetime,
    foreign key (created_by) references person (guid),
    foreign key (updated_by) references person (guid)
);

