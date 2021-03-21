package org.nachc.cad.cosmos.integration.project.build;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.nachc.cad.cosmos.action.delete.DeleteProjectAction;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjCodeDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjectDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.params.DatabricksParams;
import org.nachc.cad.cosmos.util.project.UploadDir;
import org.yaorma.dao.Dao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BuildProjectFromUploadDirIntegrationTest {

	public static final String PROJ_NAME = "integration_test";

	public static final String UID = "greshje";

	public static final String ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\_integration_test\\1942-08-01-TEST_ACME";

	/**
	 * 
	 * Delete existing project and create a new project from data files.
	 * 
	 */

	@Test
	public void shouldBuildProject() {
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("Starting test...");
			// delete the project if it exists
			log.info("Deleting existing project");
			String projRoot = DatabricksParams.getProjectFilesRoot() + PROJ_NAME;
			DeleteProjectAction.exec(PROJ_NAME, projRoot, conns);
			assertProjectExists(false, conns);
			// built the project
			log.info("Creating new Project from dir");
			UploadDir.exec(ROOT, UID, conns);
			// commit
			conns.commit();
			assertProjectExists(true, conns);
		} finally {
			conns.close();
		}
		log.info("Done.");
	}
	
	//
	// method to check that the project was correctly deleted/created
	//
	
	private void assertProjectExists(boolean exists, CosmosConnections conns) {
		ProjectDvo projDvo = Dao.find(new ProjectDvo(), "code", PROJ_NAME, conns.getMySqlConnection());
		ProjCodeDvo projCodeDvo = Dao.find(new ProjCodeDvo(), "code", PROJ_NAME, conns.getMySqlConnection());
		if(exists == true) {
			assertTrue(projDvo != null);
			assertTrue(projCodeDvo != null);
		} else {
			assertTrue(projDvo == null);
			assertTrue(projCodeDvo == null);
		}
	}

}
