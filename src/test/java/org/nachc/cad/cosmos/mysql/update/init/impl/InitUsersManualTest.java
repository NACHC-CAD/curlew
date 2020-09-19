package org.nachc.cad.cosmos.mysql.update.init.impl;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Data;
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
		int before = getCnt(conn);
		log.info("Count: " + before);
		log.info("Inserting users");
		InitUsers.init(conn);
		log.info("Doing commit");
		Database.commit(conn);
		log.info("Done.");
	}
	
	private int getCnt(Connection conn) {
		Data data = Database.query("select count(*) cnt from cosmos.person", conn);
		int cnt = data.get(0).getInt("cnt");
		return cnt;
	}
	
}
