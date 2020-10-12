package org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.impl.AddAllFilesToDatabricks;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddAllProcThumb {

	@Test
	public void shouldUploadFile() {
		log.info("Starting test...");
		String rootDirName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\thumb\\proc";
		AddAllFilesToDatabricks.AddAllFiles(getParams(), rootDirName);
		log.info("Done.");
	}

	public static RawDataFileUploadParams getParams() {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setDataGroupName("Procedure");
		params.setDataGroupAbr("proc");
		params.setDatabricksFileLocation("/FileStore/tables/integration-test/womens-health/proc");
		params.setCreatedBy("greshje");
		params.setProjCode("womens_health");
		params.setProtocolNamePretty("Women's Health");
		return params;
	}
}
