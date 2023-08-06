package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update99999999GrantPrivileges {

	public static void main(String[] args) {
		log.info("Staring Grant Privileges...");
		CosmosConnections conns = CosmosConnections.getConnections();
		try {
			log.info("Doing grants");
			Database.update("grant usage, select on database womens_health to `users`", conns.getDbConnection());
			Database.update("grant usage, select on database covid to `users`", conns.getDbConnection());
			Database.update("grant usage, select on database covid_meta to `users`", conns.getDbConnection());
			// Database.update("grant usage, select on database loinc to `users`", conns.getDbConnection());
		} finally {
			conns.close();
		}
		log.info("Done with grant privileges...");
	}
	
}
