package org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.impl.AddAllFilesToDatabricks;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddAllObsThumb {

	@Test
	public void shouldUploadFile() {
		log.info("Starting test...");
		String rootDirName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\thumb\\obs";
		AddAllFilesToDatabricks.AddAllFiles(getParams(), rootDirName);
		log.info("Done.");
	}

	public static RawDataFileUploadParams getParams() {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setDataGroupName("Observation");
		params.setDataGroupAbr("obs");
		params.setDatabricksFileLocation("/FileStore/tables/integration-test/womens-health/obs");
		params.setCreatedBy("greshje");
		params.setProjCode("womens_health");
		params.setProtocolNamePretty("Women's Health");
		return params;
	}
}
