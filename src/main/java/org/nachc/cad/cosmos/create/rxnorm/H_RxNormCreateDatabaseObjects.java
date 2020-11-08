package org.nachc.cad.cosmos.create.rxnorm;

import java.sql.Connection;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class H_RxNormCreateDatabaseObjects {

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
			String schemaName = A_RxNormParameters.SCHEMA_NAME;
			// create tables
			createTable(schemaName, "rxnconso", A_RxNormParameters.CONSO_DIR, "|", conn);
			createTable(schemaName, "rxnrel", A_RxNormParameters.REL_DIR, "|", conn);
			createTable(schemaName, "rxnsat", A_RxNormParameters.SAT_DIR, "|", conn);
			log.info("Done creating database objects.");
		} finally {
			Database.close(conn);
		}
	}

	private static void createTable(String schemaName, String tableName, String path, String delim, Connection conn) {
		log.info("Dropping table (" + schemaName + "." + tableName + ") for " + path);
		DatabricksDbUtil.dropTable(schemaName, tableName, conn);
		log.info("Creating table (" + schemaName + "." + tableName + ") for " + path);
		DatabricksDbUtil.createCsvTableForDir(path, schemaName, tableName, delim, conn);
		log.info("Done creating database objects.");
	}

}
