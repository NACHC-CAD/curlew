package org.nachc.cad.cosmos.util.connection;

import java.sql.Connection;

import javax.sql.DataSource;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;
import org.yaorma.database.DatabaseConnectionManager;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@Slf4j
public class CosmosConnections implements DatabaseConnectionManager {

	//
	// instance variables
	//
	
	private Connection mySqlConnection;

	private Connection dbConnection;

	private boolean isDsConnection = false;

	private DataSource mysqlDs;

	private DataSource databricksDs;

	private static final Object LOCK = new Object();
	
	private static int openConnections = 0;
	
	//
	// static methods
	//
	
	public static CosmosConnections open(DataSource mysqlDs, DataSource databricksDs) {
		CosmosConnections rtn = new CosmosConnections(mysqlDs, databricksDs);
		synchronized (LOCK) {
			openConnections++;
		}
		return rtn;
	}

	public static void close(CosmosConnections conns) {
		log.info("\n\n\nClosing connection using 2023-04-07 version \n\n\n");
		if(conns != null) {
			conns.close();
			synchronized (LOCK) {
				openConnections--;
			}
		}
	}
	
	public static int getOpenCount() {
		int cnt;
		synchronized (LOCK) {
			cnt = openConnections;
		}
		return cnt;
	}
	
	//
	// constructors
	//
	
	public CosmosConnections() {
		init();
	}

	public CosmosConnections(DataSource mysqlDs, DataSource databricksDs) {
		this.mysqlDs = mysqlDs;
		this.databricksDs = databricksDs;
		initDs();
	}

	//
	// public interface
	//
	
	@Override
	public void resetConnections() {
		log.info("! ! ! RESETTING CONNECTIONS ! ! !");
		if (this.isDsConnection == false) {
			log.info("Doing commit...");
			commit();
			log.info("Doing close...");
			close();
			log.info("Doing init...");
			init();
			log.info("Done with reset.");
		} else {
			close();
			initDs();
		}
	}

	@Override
	public void resetConnection(String name) {
		if ("databricks".equals(name)) {
			closeDbConnection();
			this.dbConnection = DatabricksDbConnectionFactory.getConnection();
		} else if ("mysql".equals(name)) {
			closeMySqlConnection();
			this.mySqlConnection = MySqlConnectionFactory.getCosmosConnection();
		}
	}

	@Override
	public Connection getConnection(String name) {
		if ("databricks".equals(name)) {
			return this.dbConnection;
		} else if ("mysql".equals(name)) {
			return this.mySqlConnection;
		} else {
			return null;
		}
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

	//
	// implementation (all private past here)
	//
	
	private void initDs() {
		try {
			this.isDsConnection = true;
			if(this.mysqlDs != null) {
				this.mySqlConnection = this.mysqlDs.getConnection();
			}
			if(this.databricksDs != null) {
				this.dbConnection = this.databricksDs.getConnection();
				DatabricksDbUtil.initParsePolicy(this.dbConnection);
			}
		} catch (Exception exp) {
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

	private void closeMySqlConnection() {
		if(mySqlConnection != null) {
			try {
				Database.close(mySqlConnection);
			} catch(Exception exp) {
				log.info("Closing MySql connection threw an exception (this happens sometimes)");
			}
		}
	}

	private void closeDbConnection() {
		if(this.dbConnection != null) {
			try {
				Database.close(this.dbConnection);
				this.dbConnection = null;
			} catch (Exception exp) {
				log.info("Closing Databricks connection threw an exception (this happens sometimes)");
			}
		}
	}

}
