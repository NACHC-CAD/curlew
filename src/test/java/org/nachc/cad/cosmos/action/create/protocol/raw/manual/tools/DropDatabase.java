package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.confirm.ConfirmConfiguration;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DropDatabase {

	public static final String DB_NAME = "scratch";
	
	public static void main(String[] args) {
		log.info("Confirming Configuration...");
		ConfirmConfiguration.main(null);
		log.info("Getting connection...");
		CosmosConnections conns = new CosmosConnections();
		Connection conn = conns.getDbConnection();
		log.info("Droping database schema: " + DB_NAME);
		DatabricksDbUtil.dropDatabase(DB_NAME, conn, conns);
		log.info("Done.");
	}
	
}
