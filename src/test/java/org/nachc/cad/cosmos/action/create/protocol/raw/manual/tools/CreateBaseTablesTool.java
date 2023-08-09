package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateBaseTablesAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateBaseTablesTool {

	private static final String PROJ = "womens_health";

	public static void main(String[] args) {
		log.info("Creating base tables for: " + PROJ);
		CreateBaseTablesAction.exec(PROJ);
		log.info("Done.");
	}

}
