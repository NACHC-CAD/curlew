package org.nachc.cad.cosmos.util.connection;

import java.sql.Connection;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@Slf4j
public class CosmosConnections {

	private Connection mySqlConnection;
	
	private Connection dbConnection;
	
	public CosmosConnections() {
		log.info("* * * CREATING NEW MYSQL CONNECTION * * *");
		this.mySqlConnection = MySqlConnectionFactory.getCosmosConnection();
		log.info("* * * CREATING NEW DATABRICKS CONNECTION * * *");
		this.dbConnection = DatabricksDbConnectionFactory.getConnection();
		log.info("Done getting connections");
	}
	
	public void commit() {
		Database.commit(this.mySqlConnection);
	}
	
	public void close() {
		log.info("! ! ! CLOSING MYSQL CONNECTION ! ! !");
		Database.close(mySqlConnection);
		log.info("! ! ! CLOSING DATABRICKS CONNECTION ! ! !");
		Database.close(dbConnection);
		log.info("Done closing connections");
	}
	
}
