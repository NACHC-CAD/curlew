package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build;

import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;

public class BuildParamsWomensHealthPostPartum {

	public static final String SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health-post-partum\\build\\";

	public static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/prod/womens-health-post-partum/";

	public static RawDataFileUploadParams getParams(String name, String abr) {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setCreatedBy("greshje");
		params.setProjCode("womens_health_pp");
		params.setProtocolNamePretty("Women's Health Post Partum");
		params.setDataLot("LOT 1");
		params.setDatabricksFileLocation(DATABRICKS_FILE_ROOT + abr);
		params.setDataGroupName(name);
		params.setDataGroupAbr(abr);
		String localHostFileAbsLocation = SRC_ROOT + abr;
		params.setLocalHostFileAbsLocation(localHostFileAbsLocation);
		return params;
	}

}
