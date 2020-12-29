package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import java.sql.Connection;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DropDatabase {

	public static void main(String[] args) {
		String dbName = "womens_health";
		log.info("Getting connection...");
		Connection conn = DatabricksDbConnectionFactory.getConnection();
		log.info("Droping database schema: " + dbName);
		DatabricksDbUtil.dropDatabase(dbName, conn);
		log.info("Done.");
	}
	
}
