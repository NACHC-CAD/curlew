-- 
-- add foreign key to raw_table_file
-- 

use cosmos;

alter table raw_table_file add (
	foreign key (raw_table) references raw_table(guid)
);

