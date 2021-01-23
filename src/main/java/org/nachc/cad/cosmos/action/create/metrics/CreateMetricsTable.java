package org.nachc.cad.cosmos.action.create.metrics;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.database.Data;
import org.yaorma.database.Database;
import org.yaorma.database.Row;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateMetricsTable {

	public static void createMetaSchema(String schemaName, String metaSchema, CosmosConnections conns) {
		Data data = Database.query("show tables in " + schemaName, conns.getDbConnection());
		for(Row row : data) {
			String tableName = row.get("tablename");
			log.info("Creating table: " + tableName);
			String sqlString = "create table " + metaSchema + "." + tableName + "_meta using delta as (\n";
			sqlString += getMetricsTableSqlString(schemaName, tableName, conns) + "\n";
			sqlString += ")\n";
			log.info("SqlString: \n\n" + sqlString + "\n\n");
			Database.update(sqlString, conns.getDbConnection());
		}
		
	}
	
	public static String getMetricsTableSqlString(String tableSchema, String tableName, CosmosConnections conns) {
		Data data = Database.query("show columns in " + tableSchema + "." + tableName, conns.getDbConnection());
		String sqlString = "";
		int cnt = 0;
		for (Row row : data) {
			if ("".equals(sqlString) == false) {
				sqlString += "union all\n";
			}
			cnt++;
			String colName = row.get("colName");
			sqlString += "select \n";
			sqlString += "  " + cnt + " col_position, \n";
			sqlString += "  '" + colName + "' col_name, \n";
			sqlString += "  count(*) all_records, \n";
			sqlString += "  count(" + colName + ") not_null, \n";
			sqlString += "  count(*) - count(" + colName + ") null, \n";
			sqlString += "  count(distinct " + colName + ") distinct_values \n";
			sqlString += "from \n";
			sqlString += "  " +  tableSchema + "." + tableName + " \n";

		}
		return sqlString;
	}

}
