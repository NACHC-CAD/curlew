package org.nachc.cad.cosmos.action.create.protocol.raw.manual;

import java.sql.Connection;
import java.util.List;

import org.nachc.cad.cosmos.mysql.update.UpdateMySql;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

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
		DatabricksFileUtilFactory.get().rmdir("/FileStore/tables");
		List<String> schema = DatabricksDbUtil.listRawSchema(dbConn);
		for (String str : schema) {
			log.info("Dropping schema: " + str);
			DatabricksDbUtil.dropDatabase(str, dbConn);
		}
	}

	private static void doMySql() {
		log.info("Getting MySql connection");
		Connection mySqlConn = MySqlConnectionFactory.getMysqlConnection("");
		log.info("Dropping MySql");
		Database.update("drop schema if exists cosmos", mySqlConn);
		UpdateMySql.update(mySqlConn);
	}

}
