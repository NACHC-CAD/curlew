package org.nachc.cad.cosmos.action.create.protocol.raw.manual;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.util.AddRawDataFileIntegrationTestUtil;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BurnEverythingToTheGround {

	public static void main(String[] args) {
		log.info("* * * BURNING EVERYTHING TO THE GROUND * * *");
		doDatabricks();
		doMySql();
		log.info("Done.");
	}

	private static void doDatabricks() {
		log.info("Getting Databricks connection");
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		log.info("Dropping Databricks");
		AddRawDataFileIntegrationTestUtil.burnDatabricksToTheGround(dbConn);
	}

	private static void doMySql() {
		log.info("Getting MySql connection");
		Connection mySqlConn = MySqlConnectionFactory.getMysqlConnection("");
		log.info("Dropping MySql");
		AddRawDataFileIntegrationTestUtil.burnMySqlToTheGround(mySqlConn);
	}

}
