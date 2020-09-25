package org.nachc.cad.cosmos.mysql.drop;

import java.sql.Connection;

import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DropMySqlSchema {

	public static void main(String[] args) {
		log.info("* * * * * * * * * * * * * * * * * * * * * * *");
		log.info("* * *                                   * * *");
		log.info("* * *    ABOUT TO DROP MYSQL SCHEMA     * * *");
		log.info("* * *                                   * * *");
		log.info("* * * * * * * * * * * * * * * * * * * * * * *");
		log.info("Getting connection...");
		Connection conn = MySqlConnectionFactory.getMysqlConnection("");
		log.info("Droppig database");
		Database.update("drop schema if exists cosmos", conn);
		log.info("Done.");
	}

}
