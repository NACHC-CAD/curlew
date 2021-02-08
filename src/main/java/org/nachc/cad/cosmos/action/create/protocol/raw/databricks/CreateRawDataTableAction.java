package org.nachc.cad.cosmos.action.create.protocol.raw.databricks;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.RawTableProxy;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawDataTableAction {

	public static void execute(RawDataFileUploadParams params, CosmosConnections conns, boolean isOverwrite) {
		Connection dbConn = conns.getDbConnection();
		String schemaName = params.getRawTableDvo().getSchemaName();
		String tableName = params.getRawTableName();
		String rawSchemaName = params.getRawTableSchemaName();
		String grpSchemaName = params.getGroupTableSchemaName();
		// create the raw schema if it does not exist
		if(DatabricksDbUtil.databaseExists(rawSchemaName, dbConn, conns) == false) {
			DatabricksDbUtil.createDatabase(rawSchemaName, dbConn);
		}
		// create the group schema if it does not exist
		if(DatabricksDbUtil.databaseExists(grpSchemaName, dbConn, conns) == false) {
			DatabricksDbUtil.createDatabase(grpSchemaName, dbConn);
		}
		DatabricksDbUtil.dropTable(schemaName, tableName, dbConn);
		// String sqlString = RawTableProxy.getDatabricksCreateTableSqlString(dvo, fileDvo, cols);
		String sqlString = RawTableProxy.getDatabricksCreateTableSqlString(params.getRawTableDvo(), params.getRawTableFileDvo(), params.getRawTableColList(), params.getDelimiter());
		log.info("Got sqlString: \n\n" + sqlString + "\n\n");
		if(isOverwrite == true) {
			log.info("Dropping existing table...");
			DatabricksDbUtil.dropTable(params.getRawTableSchemaName(), params.getRawTableName(), dbConn);
			log.info("Done with drop.");
		}
		log.info("Doing update...");
		Database.update(sqlString, dbConn);
		log.info("Table created");
	}

}
