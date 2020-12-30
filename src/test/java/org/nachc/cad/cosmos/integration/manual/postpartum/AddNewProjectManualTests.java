package org.nachc.cad.cosmos.integration.manual.postpartum;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201228AddPostPartumProject;
import org.nachc.cad.cosmos.action.delete.DeleteProjectAction;
import org.nachc.cad.cosmos.action.delete.DeleteRawDataGroupAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddNewProjectManualTests {

	@Test
	public void shouldDeleteAndCreateProject() {
		log.info("Starting test...");
		log.info("Getting connections");
		CosmosConnections conns = new CosmosConnections();
		try {
			// delete the project
			deleteProject(conns);
			conns.commit();
			// add the project
			Update20201228AddPostPartumProject.exec(conns);
			conns.commit();
		} finally {
			conns.close();
		}
		log.info("Done");
	}

	private void deleteProject(CosmosConnections conns) {
		DeleteRawDataGroupAction.delete("womens_health_pp_enc", conns);
		DeleteRawDataGroupAction.delete("womens_health_pp_rx", conns);
		DeleteRawDataGroupAction.delete("womens_health_pp_demo", conns);
		DeleteProjectAction.delete("womens_health_pp", conns);
	}

}
