package org.nachc.cad.cosmos.util.connection;

import java.sql.Connection;

import javax.sql.DataSource;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;
import org.yaorma.database.DatabaseConnectionManager;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@Slf4j
public class CosmosConnections implements DatabaseConnectionManager {

	private Connection mySqlConnection;
	
	private Connection dbConnection;
	
	public CosmosConnections() {
		init();
	}

	public CosmosConnections(DataSource mysqlDs, DataSource databricksDs) {
		try {
			this.mySqlConnection = mysqlDs.getConnection();
			this.dbConnection = databricksDs.getConnection();
		} catch(Exception exp) {
			throw new RuntimeException(exp);
		}
	}

	private void init() {
		log.info("* * * CREATING NEW MYSQL CONNECTION * * *");
		this.mySqlConnection = MySqlConnectionFactory.getCosmosConnection();
		log.info("* * * CREATING NEW DATABRICKS CONNECTION * * *");
		this.dbConnection = DatabricksDbConnectionFactory.getConnection();
		log.info("Done getting connections");
	}
	
	public void commit() {
		Database.commit(this.mySqlConnection);
	}
	
	public void rollback() {
		Database.rollback(this.mySqlConnection);
	}
	
	public void close() {
		log.info("! ! ! CLOSING MYSQL CONNECTION ! ! !");
		closeMySqlConnection();
		log.info("! ! ! CLOSING DATABRICKS CONNECTION ! ! !");
		closeDbConnection();
		log.info("Done closing connections");
	}

	private void closeMySqlConnection() {
		Database.close(mySqlConnection);
	}
	
	private void closeDbConnection() {
		try {
			Database.close(dbConnection);
		} catch(Exception exp) {
			log.info("Closing Databricks connection threw and exception (this happens sometimes)");
		}
	}
	
	@Override
	public void resetConnections() {
		close();
		init();
	}

	@Override
	public void resetConnection(String name) {
		if("databricks".equals(name)) {
			closeDbConnection();
			this.dbConnection = DatabricksDbConnectionFactory.getConnection();
		} else if("mysql".equals(name)) {
			closeMySqlConnection();
			this.mySqlConnection = MySqlConnectionFactory.getCosmosConnection();
		}
	}

	@Override
	public Connection getConnection(String name) {
		if("databricks".equals(name)) {
			return this.dbConnection;
		} else if("mysql".equals(name)) {
			return this.mySqlConnection;
		} else {
			return null;
		}
	}

}
