package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.integrationtest;

import org.nachc.cad.cosmos.action.delete.DeleteProjectAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.params.DatabricksParams;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BuildIntegrationTestProject {

	/**
	 *
	 * This method will delete and rebuild the integration test project. 
	 *
	 */
	
	public static void exec(CosmosConnections conns) {
		DeleteProjectAction.exec("integration_test", DatabricksParams.getProjectFilesRoot(), conns);
		
	}
	
	//
	// implementation
	//
	
	public static void main(String[] args) {
		log.info("Starting main...");
		log.info("Getting connection...");
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("executing...");
			exec(conns);
			log.info("Committing");
			conns.commit();
			log.info("Done with commit");
		} finally {
			log.info("Closing connections...");
			conns.close();
			log.info("Done with close");
		}
		log.info("Done building project.");
	}

}
