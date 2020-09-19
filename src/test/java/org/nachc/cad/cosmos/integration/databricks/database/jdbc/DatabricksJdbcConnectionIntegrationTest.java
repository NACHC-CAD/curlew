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
		String sqlString = "show databases";
		log.info("Excecuting query: " + sqlString);
		Data data = Database.query(sqlString, conn);
		log.info("Got response:");
		for(Map<String, String> row : data) {
			log.info("\t" + row.get("namespace"));
		}
		assertTrue(data.size() > 0);
		log.info("Done.");
	}
	
}
