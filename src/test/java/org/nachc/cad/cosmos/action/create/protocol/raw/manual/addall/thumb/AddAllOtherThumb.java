package org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.impl.AddAllFilesToDatabricks;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddAllOtherThumb {

	@Test
	public void shouldUploadFile() {
		log.info("Starting test...");
		String rootDirName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\thumb\\other";
		AddAllFilesToDatabricks.AddAllFiles(getParams(), rootDirName);
		log.info("Done.");
	}

	public static RawDataFileUploadParams getParams() {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setDataGroupName("Other");
		params.setDataGroupAbr("other");
		params.setDatabricksFileLocation("/FileStore/tables/integration-test/womens-health/other");
		params.setCreatedBy("greshje");
		params.setProjCode("womens_health");
		params.setProtocolNamePretty("Women's Health");
		return params;
	}
}
