package org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.impl.AddAllFilesToDatabricks;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddAllRxThumb {

	@Test
	public void shouldUploadFile() {
		log.info("Starting test...");
		String rootDirName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\thumb\\rx";
		AddAllFilesToDatabricks.AddAllFiles(getParams(), rootDirName);
		log.info("Done.");
	}

	public static RawDataFileUploadParams getParams() {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setDataGroupName("Rx");
		params.setDataGroupAbr("rx");
		params.setDatabricksFileLocation("/FileStore/tables/integration-test/womens-health/rx");
		params.setCreatedBy("greshje");
		params.setProjCode("womens_health");
		params.setProtocolNamePretty("Women's Health");
		return params;
	}
}
