package org.nachc.cad.cosmos.create.valueset;

import java.sql.Connection;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class H_CreateValueSetDatabaseObjects {

	public static void main(String[] args) {
		create();
	}

	public static void create() {
		Connection conn = null;
		try {
			// get connection
			log.info("Getting connection...");
			conn = DatabricksDbConnectionFactory.getConnection();
			log.info("Got connection");
			String schemaName = A_ParametersForValueSetSchema.SCHEMA_NAME;
			String path;
			String tableName;
			// create value_set table
			tableName = "value_set";
			path = A_ParametersForValueSetSchema.DATABRICKS_FILE_PATH;
			log.info("Dropping table (" + schemaName + "." + tableName + ") for " + path);
			DatabricksDbUtil.dropTable(schemaName, tableName, conn);
			log.info("Creating table (" + schemaName + "." + tableName + ") for " + path);
			DatabricksDbUtil.createCsvTableForDir(path, schemaName, tableName, conn);
			log.info("Done creating database objects.");
			// create value_set_meta table
			tableName = "value_set_meta";
			path = A_ParametersForValueSetSchema.DATABRICKS_META_FILE_PATH;
			log.info("Dropping table (" + schemaName + "." + tableName + ") for " + path);
			DatabricksDbUtil.dropTable(schemaName, tableName, conn);
			log.info("Creating table (" + schemaName + "." + tableName + ") for " + path);
			DatabricksDbUtil.createCsvTableForDir(path, schemaName, tableName, conn);
			log.info("Done creating database objects.");
		} finally {
			Database.close(conn);
		}
	}

}
