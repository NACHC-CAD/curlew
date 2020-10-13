package org.nachc.cad.cosmos.action.create.protocol.raw.databricks;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableColDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableFileDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.yaorma.dao.Dao;
import org.yaorma.database.Data;
import org.yaorma.database.Database;
import org.yaorma.database.Row;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateGrpDataTableAction {

	public static void execute(RawDataFileUploadParams params, Connection dbConn, Connection mySqlConn) {
		execute(params, dbConn, mySqlConn, false);
	}

	public static void execute(RawDataFileUploadParams params, Connection dbConn, Connection mySqlConn, boolean refresh) {
		// get the group
		log.info("Getting group");
		RawTableGroupDvo tableGroup = Dao.find(new RawTableGroupDvo(), "code", params.getRawTableGroupCode(), mySqlConn);
		log.info("Got group with guid: " + tableGroup.getGuid());
		// get the distinct columns
		List<String> groupCols = getGroupColumns(tableGroup.getGuid(), mySqlConn);
		// get the tables in the group
		log.info("Getting tables in group");
		List<RawTableDvo> tables = Dao.findList(new RawTableDvo(), "raw_table_group", tableGroup.getGuid(), mySqlConn);
		log.info("Got " + tables.size() + " tables");
		String sqlString = "create table " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName() + " as \n";
		for (RawTableDvo table : tables) {
			if (sqlString.endsWith(" as \n") == false) {
				sqlString += "\n\nunion all \n\n";
			}
			String tableSql = getQueryStringForTable(params, table.getGuid(), groupCols, mySqlConn);
			log.info("GOT SQL STRING: \n\n" + tableSql + "\n\n");
			sqlString += tableSql;
		}
		log.info("SQL STRING: \n\n" + sqlString + "\n\n");
		log.info("DROPPING table: " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName());
		// create the table in databricks
		Database.update("drop table if exists " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName(), dbConn);
		log.info("CREATING table: " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName());
		Database.update(sqlString, dbConn);
		if (refresh == true) {
			log.info("Refreshing table: " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName());
			Database.update("refresh table " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName(), dbConn);
		}
		log.info("Done.");
	}

	//
	// method to get the distinct list of columns for a group
	//

	private static List<String> getGroupColumns(String groupGuid, Connection conn) {
		String sqlString = "";
		sqlString += "select \n";
		sqlString += "	distinct coalesce(col.col_alias, col.col_name) col_name \n";
		sqlString += "from \n";
		sqlString += "	raw_table_group grp \n";
		sqlString += "  join raw_table raw on raw.raw_table_group = grp.guid \n";
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

	private static String getQueryStringForTable(RawDataFileUploadParams params, String rawTableGuid, List<String> groupCols, Connection conn) {
		log.info("Creating query for table with guid: " + rawTableGuid);
		RawTableDvo tableDvo = Dao.find(new RawTableDvo(), "guid", rawTableGuid, conn);
		RawTableFileDvo fileDvo = Dao.find(new RawTableFileDvo(), "raw_table", rawTableGuid, conn);
		List<RawTableColDvo> tableCols = Dao.findList(new RawTableColDvo(), "raw_table", rawTableGuid, conn);
		log.info("Got " + tableCols.size() + " cols for guid: " + rawTableGuid);
		String sqlString = "";
		sqlString += "select \n";
		for (String colName : groupCols) {
			RawTableColDvo dvo;
			dvo = getAsAlias(colName, tableCols);
			if (dvo != null) {
				sqlString += "  " + dvo.getColName() + " as " + dvo.getColAlias() + ", \n";
				continue;
			}
			dvo = getAsName(colName, tableCols);
			if (dvo != null) {
				sqlString += "  " + dvo.getColName() + ", \n";
				continue;
			}
			sqlString += "  null as " + colName + ", \n";
		}
		sqlString += "'" + fileDvo.getOrgCode() + "' as org, \n";
		sqlString += "'" + tableDvo.getRawTableName() + "' as raw_table \n";
		sqlString += "from \n";
		sqlString += "  " + tableDvo.getRawTableSchema() + "." + tableDvo.getRawTableName();
		return sqlString;
	}

	private static RawTableColDvo getAsAlias(String colName, List<RawTableColDvo> tableCols) {
		for (RawTableColDvo dvo : tableCols) {
			if (colName.equals(dvo.getColAlias())) {
				return dvo;
			}
		}
		return null;
	}

	private static RawTableColDvo getAsName(String colName, List<RawTableColDvo> tableCols) {
		for (RawTableColDvo dvo : tableCols) {
			if (colName.equals(dvo.getColName())) {
				return dvo;
			}
		}
		return null;
	}

}
