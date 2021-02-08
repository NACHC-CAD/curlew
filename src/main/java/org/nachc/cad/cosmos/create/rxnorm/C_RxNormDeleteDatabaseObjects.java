package org.nachc.cad.cosmos.create.rxnorm;

import java.sql.Connection;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class C_RxNormDeleteDatabaseObjects {

	public static void delete() {
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("Getting connection...");
			Connection conn = DatabricksDbConnectionFactory.getConnection();
			String schemaName = A_RxNormParameters.SCHEMA_NAME;
			log.info("Dropping database: " + schemaName);
			DatabricksDbUtil.dropDatabase(schemaName, conn, conns);
			log.info("Done dropping: " + schemaName);
		} finally {
			conns.close();
		}

	}
}
