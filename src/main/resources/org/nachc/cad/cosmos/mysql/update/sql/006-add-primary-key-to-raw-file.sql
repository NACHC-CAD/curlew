-- 
-- add foreign key to raw_table_file
-- 

use cosmos;

alter table raw_table_file add (
	primary key (raw_table)
);

