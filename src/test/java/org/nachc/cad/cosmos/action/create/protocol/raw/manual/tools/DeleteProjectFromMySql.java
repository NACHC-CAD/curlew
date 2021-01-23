package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.action.delete.DeleteProjectFromMySqlAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteProjectFromMySql {

	public static void main(String[] args) {
		log.info("Deleteing project from mysql...");
		log.info("Getting connections...");
		CosmosConnections conns = new CosmosConnections();
		log.info("Doing delete");
		DeleteProjectFromMySqlAction.delete("loinc", conns);
		DeleteProjectFromMySqlAction.delete("covid", conns);
		DeleteProjectFromMySqlAction.delete("covid_meta", conns);
		conns.commit();
		log.info("Done.");
	}

}
