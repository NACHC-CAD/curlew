package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools.downloadfiles.test;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools.downloadfiles.DownloadFilesTool;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DownloadFilesToolIntegrationTest {

	@Test
	public void shouldGetFile() {
		log.info("Starting test...");
		String sqlString = getFilesSqlString();
		DownloadFilesTool.exec(sqlString);
		log.info("Done.");
	}

	private String getFilesSqlString() {
		String sqlString = "";
		sqlString += "select distinct \n";
		sqlString += "	project, \n";
		sqlString += "    group_table_name, \n";
		sqlString += "    org_code, \n";
		sqlString += "    data_lot, \n";
		sqlString += "    file_location, \n";
		sqlString += "    file_name, \n";
		sqlString += "    file_size, \n";
		sqlString += "    file_size_units, \n";
		sqlString += "    provided_by, \n";
		sqlString += "    provided_date, \n";
		sqlString += "    file_created_date \n";
		sqlString += "from \n";
		sqlString += "	raw_table_col_detail \n";
		sqlString += "where \n";
		sqlString += "	project = 'womens_health' \n";
		sqlString += "  and data_lot != 'LOT 1' \n";
		sqlString += "  and file_name = '2021_04_08_WHPP_WHC_SK-DEMO.csv' \n";
		sqlString += "and \n";
		sqlString += "	org_code in ('ac','ochin') \n";
		sqlString += "order by project, org_code, group_table_name, file_name \n";
		return sqlString;
	}

}
