-- * * *
--
-- BASIC REPORT QUERIES
--
-- * * *

use cosmos;

--
-- files for project
--

select distinct
    org_code org,
    data_lot "Data Lot",
    group_table_name "Data Group",
	file_name "File Name",
    format(file_size,0) "File Size (b)"
from
	raw_table_col_detail
where 1=1
	and project = 'covid'
order by 1,2,3
;