package org.nachc.cad.cosmos.integration.databricks.database.jdbc;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.Map;

import org.junit.Test;
import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
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
		String url = DatabricksParams.getJdbcUrl();
		log.info("Got url: " + url);
		String token = DatabricksAuthUtil.getToken();
		log.info("Got token: " + token);
		Connection conn = DatabricksDbUtil.getConnection(url, token);
		log.info("Got connection");
		String sqlString; 
		Data data;
		// simple query to test connectivity
		sqlString = "show databases";
		log.info("Excecuting query: " + sqlString);
		data = Database.query(sqlString, conn);
		log.info("Got response:");
		for(Map<String, String> row : data) {
			log.info("\t" + row.get("namespace"));
		}
		assertTrue(data.size() > 0);
		// check that the schema has been initialized
		sqlString = "select * from cosmos.build_version";
		data = Database.query(sqlString, conn);
		assertTrue("Could not get data from build_version (DB not initiated)", data.size() > 0);
		log.info("Got response:");
		for(Map<String, String> row : data) {
			log.info("\t" + row.get("versionNumber") + "\t" + row.get("fileName"));
		}
		log.info("Done.");
	}
	
}
