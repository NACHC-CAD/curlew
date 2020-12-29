package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.refresh.RefreshSchemaAction;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RefreshSchema {

	private static String SCHEMA = "womens_health";
	
	public static void main(String[] args) {
		log.info("Getting connection");
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		log.info("Doing refresh");
		RefreshSchemaAction.exec(SCHEMA, dbConn);
		log.info("Done.");
	}
	
}
