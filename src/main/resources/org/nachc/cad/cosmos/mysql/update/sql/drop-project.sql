--
-- script to delete records for a project
--

use cosmos;

show tables;

delete from raw_table_col where project = 'hiv';

delete from raw_table_file where project = 'hiv';

delete from raw_table where project = 'hiv';

delete from raw_table_group where project = 'hiv';



