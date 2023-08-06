package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateGroupTableTool {

	private static final String CODE = "womens_health_fert";
	
	public static void main(String[] args) {
		log.info("Updating group table: " + CODE);
		log.info("Getting connections...");
		CosmosConnections conns = CosmosConnections.getConnections();
		try {
			log.info("Creating group table");
			CreateGrpDataTableAction.execute(CODE, conns);
			log.info("Create group table finished.");
		} finally {
			conns.close();
		}
		log.info("Done.");
	}
	
}
