package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;

public class BuildParams {

	public static final String SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\thumb\\";

	public static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/integration-test/womens-health/";

	public static RawDataFileUploadParams getParams(String name, String abr) {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setCreatedBy("greshje");
		params.setProjCode("womens_health");
		params.setProtocolNamePretty("Women's Health");
		params.setProjCode("womens_health");
		params.setDataLot("LOT 1");
		params.setDatabricksFileLocation(BuildParams.DATABRICKS_FILE_ROOT + abr);
		params.setDataGroupName(name);
		params.setDataGroupAbr(abr);
		String localHostFileAbsLocation = BuildParams.SRC_ROOT + abr;
		params.setLocalHostFileAbsLocation(localHostFileAbsLocation);
		return params;
	}

}
