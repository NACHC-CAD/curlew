package org.nachc.cad.cosmos.integration.project.build;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.integrationtest.BuildIntegrationTestProject;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BuildProjectIntegrationTest {

	@Test
	public void shouldBuildProject() {
		log.info("Starting test...");
		BuildIntegrationTestProject.main(null);
		log.info("Done.");
	}
	
}
