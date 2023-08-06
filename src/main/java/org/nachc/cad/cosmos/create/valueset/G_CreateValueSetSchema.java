package org.nachc.cad.cosmos.create.valueset;

import java.sql.Connection;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class G_CreateValueSetSchema {

	public static void main(String[] args) {
		create();
	}

	public static void create() {
		CosmosConnections conns = CosmosConnections.getConnections();
		try {
			log.info("Getting connection...");
			Connection conn = conns.getDbConnection();
			String schemaName = A_ParametersForValueSetSchema.SCHEMA_NAME;
			log.info("Dropping schema: " + schemaName);
			DatabricksDbUtil.dropDatabase(schemaName, conn, conns);
			log.info("Creating schema: " + schemaName);
			DatabricksDbUtil.createDatabase(schemaName, conn);
			log.info("Done creating schema: " + schemaName);
		} finally {
			conns.close();
		}
	}

}
