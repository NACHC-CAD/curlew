package org.nachc.cad.cosmos.action.confirm;

import java.sql.Connection;

import org.nachc.cad.cosmos.util.configuration.ConfigurationUtil;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.nachc.cad.cosmos.util.params.DatabricksParams;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfirmConfiguration {

	public static void main(String[] args) {
		log.info("Getting databricks connection...");
		CosmosConnections conns = new CosmosConnections();
		try {
			exec(conns);
		} finally {
			conns.close();
		}
	}

	public static void exec(CosmosConnections conns) {
		Connection dbConn = conns.getDbConnection();
		Connection mySqlConn = conns.getMySqlConnection();
		log.info("Starting configuration test...");
		String databricksFiles = ConfigurationUtil.getDatabricksFileStoreInstance();
		String databricksDb = ConfigurationUtil.getDatabricksSqlInstance(conns);
		String mySql = ConfigurationUtil.getMySqlInstance(mySqlConn);
		String msg = "\n\n* * * CONFIGURATION * * *";
		msg += "\nDatabricks Files:    " + databricksFiles;
		msg += "\nDatabricks DB:       " + databricksDb;
		msg += "\nMySql DB:            " + mySql;
		msg += "\n";
		msg += "\nDatabricks REST URL: " + DatabricksParams.getRestUrl(); 
		msg += "\nDatabricks JDBC URL: " + DatabricksParams.getJdbcUrl();
		msg += "\nDatabricks Token:    " + DatabricksAuthUtil.getToken();
		msg += "\n\n";
		log.info("Configuration..." + msg);
		log.info("Done.");
	}

	
}
