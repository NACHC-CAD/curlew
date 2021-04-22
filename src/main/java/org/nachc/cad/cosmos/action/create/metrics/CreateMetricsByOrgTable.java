package org.nachc.cad.cosmos.action.create.metrics;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.database.Data;
import org.yaorma.database.Database;
import org.yaorma.database.Row;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateMetricsByOrgTable {

	public static void exec(String schemaName, String metaSchema, CosmosConnections conns) {
		DatabricksDbUtil.dropDatabase(metaSchema, conns);
		DatabricksDbUtil.createDatabase(metaSchema, conns);
		Data data = Database.query("show tables in " + schemaName, conns.getDbConnection());
		for(Row row : data) {
			String tableName = row.get("tablename");
			log.info("Creating table: " + tableName);
			String metricsTableSqlString =  getMetricsTableSqlString(schemaName, tableName, conns);
			if(metricsTableSqlString == null) {
				continue;
			} else {
				String sqlString = "create table " + metaSchema + "." + tableName + "_meta using delta as (\n";
				sqlString += metricsTableSqlString + "\n";
				sqlString += ")\n";
				log.info("SqlString: \n\n" + sqlString + "\n\n");
				log.info("Creating table (from above sqlString): " + tableName);
				Database.update(sqlString, conns.getDbConnection());
			}
		}
		
	}
	
	public static String getMetricsTableSqlString(String tableSchema, String tableName, CosmosConnections conns) {
		Data data = Database.query("show columns in " + tableSchema + "." + tableName, conns.getDbConnection());
		String sqlString = "";
		int cnt = 0;
		boolean orgColumnExists = false;
		for (Row row : data) {
			if ("".equals(sqlString) == false) {
				sqlString += "union all\n";
			}
			cnt++;
			String colName = row.get("colName");
			if("org".contentEquals(colName)) {
				orgColumnExists = true;
			}
			sqlString += "select \n";
			sqlString += "  org,\n";
			sqlString += "  " + cnt + " col_position, \n";
			sqlString += "  '" + colName + "' col_name, \n";
			sqlString += "  count(*) all_records, \n";
			sqlString += "  count(" + colName + ") not_null, \n";
			sqlString += "  count(*) - count(" + colName + ") null, \n";
			sqlString += "  count(distinct " + colName + ") distinct_values \n";
			sqlString += "from \n";
			sqlString += "  " +  tableSchema + "." + tableName + " \n";
			sqlString += "group by 1,2,3\n";
		}
		if(orgColumnExists == true) {
			return sqlString;
		} else {
			return null;
		}
	}

}
