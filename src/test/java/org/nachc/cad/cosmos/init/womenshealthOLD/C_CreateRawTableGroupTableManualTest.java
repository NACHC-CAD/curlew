package org.nachc.cad.cosmos.init.womenshealthOLD;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class C_CreateRawTableGroupTableManualTest {
/*
	@Test
	public void shouldCreateGroupTable() {
		log.info("Starting test...");
		// get the mySql connection
		log.info("Getting MySql connection...");
		Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
		// get the group
		log.info("Getting group");
		RawTableGroupDvo tableGroup = Dao.find(new RawTableGroupDvo(), "code", "WOMENS_HEALTH_DEM", mySqlConn);
		log.info("Got group with guid: " + tableGroup.getGuid());
		// get the distinct columns
		List<String> groupCols = getGroupColumns(tableGroup.getGuid(), mySqlConn);
		// get the tables in the group
		log.info("Getting tables in group");
		List<RawTableGroupRawTableDvo> tables = Dao.findList(new RawTableGroupRawTableDvo(), "raw_table_group", tableGroup.getGuid(), mySqlConn);
		log.info("Got " + tables.size() + " tables");
		String sqlString = "create table " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName() + " as \n";
		for (RawTableGroupRawTableDvo table : tables) {
			if(sqlString.endsWith(" as \n") == false) {
				sqlString += "\n\nunion all \n\n";
			}
			String tableSql = getQueryStringForTable(table.getRawTable(), groupCols, mySqlConn);
			log.info("GOT SQL STRING: \n\n" + tableSql + "\n\n");
			sqlString += tableSql;
		}
		log.info("SQL STRING: \n\n" + sqlString + "\n\n");
		log.info("DROPPING table: " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName());
		// create the table in databricks
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		Database.update("drop table if exists " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName(), dbConn);
		log.info("CREATING table: " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName());
		Database.update(sqlString, dbConn);
		log.info("Refreshing table: "+ tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName());
		Database.update("refresh table " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName(), dbConn);
		log.info("Done.");
	}

	//
	// method to get the distinct list of columns for a group
	//

	private List<String> getGroupColumns(String groupGuid, Connection conn) {
		String sqlString = "";
		sqlString += "select \n";
		sqlString += "	distinct coalesce(col.col_alias, col.col_name) col_name \n";
		sqlString += "from \n";
		sqlString += "	raw_table_group grp \n";
		sqlString += "    join raw_table_group_raw_table grptab on grp.guid = grptab.raw_table_group \n";
		sqlString += "    join raw_table raw on raw.guid = grptab.raw_table \n";
		sqlString += "	join raw_table_col col on raw.guid = col.raw_table \n";
		sqlString += "where \n";
		sqlString += "	grp.guid = ? \n";
		Data data = Database.query(sqlString, groupGuid, conn);
		List<String> rtn = new ArrayList<String>();
		for (Row row : data) {
			rtn.add(row.get("colName"));
		}
		return rtn;
	}

	//
	// method to get the query string for a member table
	//

	private String getQueryStringForTable(String rawTableGuid, List<String> groupCols, Connection conn) {
		log.info("Creating query for table with guid: " + rawTableGuid);
		RawTableDvo tableDvo = Dao.find(new RawTableDvo(), "guid", rawTableGuid, conn);
		List<RawTableColDvo> tableCols = Dao.findList(new RawTableColDvo(), "raw_table", rawTableGuid, conn);
		log.info("Got " + tableCols.size() + " cols for guid: " + rawTableGuid);
		String sqlString = "";
		sqlString += "select \n";
		for(String colName : groupCols) {
			if(sqlString.endsWith("select \n") == false) {
				sqlString += ", \n";
			}
			RawTableColDvo dvo;
			dvo = getAsAlias(colName, tableCols);
			if(dvo != null) {
				sqlString += "  " + dvo.getColName() + " as " + dvo.getColAlias();
				continue;
			}
			dvo = getAsName(colName, tableCols);
			if(dvo != null) {
				sqlString += "  " + dvo.getColName();
				continue;
			}
			sqlString += "  null as " + colName;
		}
		sqlString += "\n";
		sqlString += "from \n";
		sqlString += "  " + tableDvo.getRawTableSchema() + "." + tableDvo.getRawTableName();
		return sqlString;
	}

	private RawTableColDvo getAsAlias(String colName, List<RawTableColDvo> tableCols) {
		for(RawTableColDvo dvo : tableCols) {
			if(colName.equals(dvo.getColAlias())) {
				return dvo;
			}
		}
		return null;
	}

	private RawTableColDvo getAsName(String colName, List<RawTableColDvo> tableCols) {
		for(RawTableColDvo dvo : tableCols) {
			if(colName.equals(dvo.getColName())) {
				return dvo;
			}
		}
		return null;
	}
*/
}
