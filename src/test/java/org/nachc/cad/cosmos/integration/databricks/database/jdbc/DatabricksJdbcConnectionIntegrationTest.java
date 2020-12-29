package org.nachc.cad.cosmos.integration.databricks.database.jdbc;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.Map;

import org.junit.Test;
import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.params.DatabricksParams;
import org.yaorma.database.Data;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabricksJdbcConnectionIntegrationTest {

	@Test
	public void shouldGetConnection() {
		log.info("Starting test...");
		log.info("Getting connection");
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		log.info("Got connection: " + dbConn);
		log.info("Done.");
	}
	
}
