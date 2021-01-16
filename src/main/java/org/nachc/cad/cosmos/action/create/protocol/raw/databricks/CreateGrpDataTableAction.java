package org.nachc.cad.cosmos.action.create.protocol.raw.databricks;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableColDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableFileDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.dao.Dao;
import org.yaorma.database.Data;
import org.yaorma.database.Database;
import org.yaorma.database.Row;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateGrpDataTableAction {

	public static void execute(String rawTableGroupCode, CosmosConnections conns) {
		execute(rawTableGroupCode, conns, false);
	}

	public static void execute(String rawTableGroupCode, CosmosConnections conns, boolean refresh) {
		// get the group
		log.info("Getting group for " + rawTableGroupCode);
		RawTableGroupDvo tableGroup = Dao.find(new RawTableGroupDvo(), "code", rawTableGroupCode, conns.getMySqlConnection());
		log.info("Got group with guid: " + tableGroup.getGuid());
		// get the distinct columns
		List<String> groupCols = getGroupColumns(tableGroup.getGuid(), conns.getMySqlConnection());
		// get the tables in the group
		log.info("Getting tables in group");
		List<RawTableDvo> tables = Dao.findList(new RawTableDvo(), "raw_table_group", tableGroup.getGuid(), conns.getMySqlConnection());
		log.info("Got " + tables.size() + " tables");
		String sqlString = "create table " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName() + " using delta as \n";
		for (RawTableDvo table : tables) {
			if (sqlString.endsWith(" as \n") == false) {
				sqlString += "\n\nunion all \n\n";
			}
			String tableSql = getQueryStringForTable(table.getGuid(), groupCols, conns.getMySqlConnection());
			log.info("GOT SQL STRING: \n\n" + tableSql + "\n\n");
			sqlString += tableSql;
		}
		log.info("SQL STRING: \n\n" + sqlString + "\n\n");
		log.info("DROPPING table: " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName());
		// DROP THE TABLE IF IT EXISTS
		Database.update("drop table if exists " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName(), conns.getDbConnection());
		// CREATE THE TABLE IN DATABRICKS IF ANY FILES REMAIN
		if(tables.size() > 0) {
			log.info("initializing connection parse policy");
			DatabricksDbUtil.initParsePolicy(conns.getDbConnection());
			log.info("done initializing connection parse policy");
			log.info("CREATING table: " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName());
			Database.update(sqlString, conns.getDbConnection());
			if (refresh == true) {
				log.info("Refreshing table: " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName());
				Database.update("refresh table " + tableGroup.getGroupTableSchema() + "." + tableGroup.getGroupTableName(), conns.getDbConnection());
			}
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

	private static String getQueryStringForTable(String rawTableGuid, List<String> groupCols, Connection conn) {
		log.info("Creating query for table with guid: " + rawTableGuid);
		RawTableDvo tableDvo = Dao.find(new RawTableDvo(), "guid", rawTableGuid, conn);
		RawTableFileDvo fileDvo = Dao.find(new RawTableFileDvo(), "raw_table", rawTableGuid, conn);
		List<RawTableColDvo> tableCols = Dao.findList(new RawTableColDvo(), "raw_table", rawTableGuid, conn);
		log.info("Got " + tableCols.size() + " cols for guid: " + rawTableGuid);
		String sqlString = "";
		sqlString += "select \n";
		String patientId = null;
		for (String colName : groupCols) {
			RawTableColDvo dvo;
			dvo = getAsAlias(colName, tableCols);
			if (dvo != null) {
				if(dvo.getColAlias().endsWith("_date")) {
					// sqlString += "  if(lower(trim(" + dvo.getColName() + ")) = 'null', null, trim("+ dvo.getColName() +")) as " + dvo.getColAlias() + "_string, \n";
					String str = "";
					str += "  coalesce(\n";
					str += "    to_date((" + dvo.getColName() + "), \"yyyy-MM-dd\"),\n";
					str += "    to_date((" + dvo.getColName() + "), \"MM/dd/yy\"),\n";
					str += "    to_date((" + dvo.getColName() + "), \"MM/dd/yyyy\"),\n";
					str += "    to_date((" + dvo.getColName() + "), \"yyyyMMdd\")) as " + dvo.getColAlias() + ", \n";
					sqlString += str;
				} else {
					String alias = dvo.getColAlias();
					if("patient_id".equals(alias)) {
						alias = "org_patient_id";
						patientId = dvo.getColName();
					}
					sqlString += "  if(lower(trim(" + dvo.getColName() + ")) = 'null', null, trim("+ dvo.getColName() +")) as " + alias + ", \n";
				}
				continue;
			}
			dvo = getAsName(colName, tableCols);
			if (dvo != null) {
				if(dvo.getColName().endsWith("_date")) {
					// sqlString += "  if(lower(trim(" + dvo.getColName() + ")) = 'null', null, trim("+ dvo.getColName() +")) as " + dvo.getColName() + "_string, \n";
					String str = "";
					str += "  coalesce(\n";
					str += "    to_date((" + dvo.getColName() + "), \"yyyy-MM-dd\"),\n";
					str += "    to_date((" + dvo.getColName() + "), \"MM/dd/yy\"),\n";
					str += "    to_date((" + dvo.getColName() + "), \"MM/dd/yyyy\"),\n";
					str += "    to_date((" + dvo.getColName() + "), \"yyyyMMdd\")) as " + dvo.getColName() + ", \n";
					sqlString += str;
				} else {
					String alias = dvo.getColName();
					if("patient_id".equals(alias)) {
						alias = "org_patient_id";
						patientId = "patient_id";
					}
					sqlString += "  if(lower(trim(" + dvo.getColName() + ")) = 'null', null, trim("+ dvo.getColName() +")) as " + alias + ", \n";
				}
				continue;
			}
			if("patient_id".equals(colName)) {
				patientId = "''";
				sqlString += "  null as org_patient_id, \n";
			} else {
				sqlString += "  null as " + colName + ", \n";
			}
		}
		if(patientId != null) {
			sqlString += "  concat('" + fileDvo.getOrgCode() + "', '|', " + patientId + ") as patient_id, \n";
		}
		sqlString += "  trim('" + fileDvo.getOrgCode() + "') as org, \n";
		sqlString += "  trim('" + fileDvo.getDataLot() + "') as data_lot, \n";
		sqlString += "  trim('" + tableDvo.getRawTableName() + "') as raw_table \n";
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
