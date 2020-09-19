package org.nachc.cad.cosmos.integration.mysql.connection;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.Map;

import org.junit.Test;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Data;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MySqlConnectionIntegrationTest {

	@Test
	public void shouldGetConnection() {
		log.info("Starting test...");
		log.info("Getting connection");
		Connection conn = MySqlConnectionFactory.getCosmosConnection();
		log.info("Executing query");
		String sqlString = "select * from information_schema.tables where table_schema = 'cosmos'";
		Data data = Database.query(sqlString, conn);
		log.info("Got " + data.size() + " tables.");
		assertTrue(data.size() >= 10);
		for(Map<String, String> row : data) {
			log.info("\t" + row.get("tableName"));
		}
		log.info("Done.");
	}

}
