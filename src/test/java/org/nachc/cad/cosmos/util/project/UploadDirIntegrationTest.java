package org.nachc.cad.cosmos.util.project;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.update.Update20210121_Covid_Loinc;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadDirIntegrationTest {

	public static final String DIR = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-20210317-COVID-LabTestResultNachc";

	@Test
	public void shouldUploadFiles() {
		doDirUpload();
		doLegacyUpload();
	}

	private void doDirUpload() {
		CosmosConnections conns = new CosmosConnections();
		try {
			UploadDir.exec(DIR, "greshje", conns);
			conns.commit();
		} finally {
			conns.close();
		}
		log.info("Done.");
	}

	private void doLegacyUpload() {
		Update20210121_Covid_Loinc.main(null);
	}

}
