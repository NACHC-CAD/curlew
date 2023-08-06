package org.nachc.cad.cosmos.integration.project.build;

import org.junit.Test;
import org.nachc.cad.cosmos.action.delete.DeleteProjectAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.params.DatabricksParams;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteProjectIntegrationTest {

	private static final String PROJ_NAME = "integration_test";
	
	@Test
	public void shouldDeleteProject() {
		CosmosConnections conns = CosmosConnections.getConnections();
		try {
			log.info("Starting test...");
			// delete the project if it exists
			log.info("Deleting project: " + PROJ_NAME);
			DeleteProjectAction.exec(PROJ_NAME, DatabricksParams.getProjectFilesRoot(), conns);
			log.info("Done.");
		} finally {
			conns.close();
		}
	}
	
}
