package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.action.confirm.ConfirmConfiguration;
import org.nachc.cad.cosmos.action.delete.DeleteProjectAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteProjectTool {

	public static final String PROJECT_NAME = "omop_concepts";

	public static final String FILES_LOCATION = "/FileStore/tables/prod/" + PROJECT_NAME;

	public static void main(String[] args) {
		log.info("Confirming Configuration...");
		ConfirmConfiguration.main(null);
		log.info("Getting connection...");
		if (FILES_LOCATION.startsWith("/FileStore/tables/prod/") == false || FILES_LOCATION.length() <= "/FileStore/tables/prod/".length()) {
			throw new RuntimeException("ILLEGAL ATTEMPT TO DELETE: " + FILES_LOCATION);
		}
		CosmosConnections conns = new CosmosConnections();
		DeleteProjectAction.exec(PROJECT_NAME, FILES_LOCATION, conns);
		conns.commit();
		log.info("Done.");
	}

}
