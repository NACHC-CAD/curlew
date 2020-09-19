package org.nachc.cad.cosmos.mysql.update.init.impl;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InitUsersManualTest {

	@Test
	public void shouldInsertUser() {
		log.info("Starting test...");
		log.info("Getting connection...");
		Connection conn = MySqlConnectionFactory.getCosmosConnection();
		log.info("Getting count...");
		List<Map<String, String>> data = Database.query("select count(*) cnt from cosmos.person", conn);
		log.info("Count: " + data.get(0).get("cnt"));
		log.info("Inserting users");
		InitUsers.init(conn);
		log.info("Doing commit");
		Database.commit(conn);
		log.info("Done.");
	}
	
}
