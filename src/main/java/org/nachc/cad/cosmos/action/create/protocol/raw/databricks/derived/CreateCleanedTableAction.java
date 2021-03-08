package org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived;

import java.sql.Connection;
import java.util.List;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableColDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableFileDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateCleanedTableAction {

	public static void exec(String rawTableGuid, CosmosConnections conns) {
		RawTableDvo dvo = Dao.find(new RawTableDvo(), "guid", rawTableGuid, conns.getMySqlConnection());
		String tableName = dvo.getRawTableSchema() + "." + dvo.getRawTableName() + "_CLEANED";
		String sqlString = "";
		sqlString += "create table " + tableName + " \n as (";
		sqlString += getQueryStringForTable(dvo, conns.getMySqlConnection()) + "\n";
		sqlString += ")";
		log.info("SQL STRING: \n" + sqlString);
		// drop the table if it exists
		DatabricksDbUtil.dropTable(tableName, conns.getDbConnection());
		// create the table
		Database.update(sqlString, conns.getDbConnection());
		log.info("* * *");
		log.info("TABLE CREATED: " + tableName);
		log.info("* * *");
	}

	//
	// method to get the query string for a member table
	//

	private static String getQueryStringForTable(RawTableDvo tableDvo, Connection conn) {
		String rawTableGuid = tableDvo.getGuid();
		log.info("Creating query for table with guid: " + rawTableGuid);
		RawTableFileDvo fileDvo = Dao.find(new RawTableFileDvo(), "raw_table", rawTableGuid, conn);
		List<RawTableColDvo> tableCols = Dao.findList(new RawTableColDvo(), "raw_table", rawTableGuid, conn);
		log.info("Got " + tableCols.size() + " cols for guid: " + rawTableGuid);
		String sqlString = "";
		sqlString += "select \n";
		String patientId = null;
		for (RawTableColDvo dvo : tableCols) {
			boolean dateStringAdded = false;
			if (dvo.getColAlias() != null) {
				if (dvo.getColAlias().endsWith("_date")) {
					// sqlString += " if(lower(trim(" + dvo.getColName() + ")) = 'null', null,
					// trim("+ dvo.getColName() +")) as " + dvo.getColAlias() + "_string, \n";
					String alias = dvo.getColAlias();
					dateStringAdded = true;
					String str = "";
					str += "  coalesce(\n";
					str += "    to_date(substring(" + dvo.getColName() + ",0,10), \"yyyy-MM-dd\"),\n";
					str += "    to_date(substring(" + dvo.getColName() + ",0,10), \"MM/dd/yy\"),\n";
					str += "    to_date(substring(" + dvo.getColName() + ",0,10), \"MM/dd/yyyy\"),\n";
					str += "    to_date(substring(" + dvo.getColName() + ",0,10), \"yyyyMMdd\")) as " + dvo.getColAlias() + ", \n";
					sqlString += str;
					sqlString += "  if(lower(trim(" + dvo.getColName() + ")) = 'null', null, trim(" + dvo.getColName() + ")) as " + alias + "_string, \n";
				} else {
					String alias = dvo.getColAlias();
					if ("patient_id".equals(alias)) {
						alias = "org_patient_id";
						patientId = dvo.getColName();
					}
					sqlString += "  if(lower(trim(" + dvo.getColName() + ")) = 'null', null, trim(" + dvo.getColName() + ")) as " + alias + ", \n";
				}
				continue;
			}
			if (dvo.getColAlias() == null) {
				if (dvo.getColName().endsWith("_date")) {
					// sqlString += " if(lower(trim(" + dvo.getColName() + ")) = 'null', null,
					// trim("+ dvo.getColName() +")) as " + dvo.getColName() + "_string, \n";
					String alias = dvo.getColName();
					dateStringAdded = true;
					String str = "";
					str += "  coalesce(\n";
					str += "    to_date(substring(" + dvo.getColName() + ",0,10), \"yyyy-MM-dd\"),\n";
					str += "    to_date(substring(" + dvo.getColName() + ",0,10), \"MM/dd/yy\"),\n";
					str += "    to_date(substring(" + dvo.getColName() + ",0,10), \"MM/dd/yyyy\"),\n";
					str += "    to_date(substring(" + dvo.getColName() + ",0,10), \"yyyyMMdd\")) as " + dvo.getColName() + ", \n";
					sqlString += str;
					sqlString += "  if(lower(trim(" + dvo.getColName() + ")) = 'null', null, trim(" + dvo.getColName() + ")) as " + alias + "_string, \n";
				} else {
					String alias = dvo.getColName();
					if ("patient_id".equals(alias)) {
						alias = "org_patient_id";
						patientId = "patient_id";
					}
					sqlString += "  if(lower(trim(" + dvo.getColName() + ")) = 'null', null, trim(" + dvo.getColName() + ")) as " + alias + ", \n";
				}
				continue;
			}
			String colName = dvo.getColAlias();
			if (colName == null) {
				colName = dvo.getColName();
			}
			if ("patient_id".equals(colName)) {
				patientId = "''";
				sqlString += "  null as org_patient_id, \n";
			} else {
				sqlString += "  null as " + colName + ", \n";
			}
			if (dateStringAdded == false && colName.endsWith("_date")) {
				sqlString += "  null as " + colName + "_string, \n";
			}
		}
		if (patientId != null) {
			sqlString += "  concat('" + fileDvo.getOrgCode() + "', '|', " + patientId + ") as patient_id, \n";
		}
		sqlString += "  trim('" + fileDvo.getOrgCode() + "') as org, \n";
		sqlString += "  trim('" + fileDvo.getDataLot() + "') as data_lot, \n";
		sqlString += "  trim('" + tableDvo.getRawTableName() + "') as raw_table \n";
		sqlString += "from \n";
		sqlString += "  " + tableDvo.getRawTableSchema() + "." + tableDvo.getRawTableName();
		return sqlString;
	}

}
