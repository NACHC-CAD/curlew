package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.create;

import org.nachc.cad.cosmos.action.create.project.CreateProjectAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

public class CreateCovidProject {

	private static final String PROJECT_CODE = "covid";

	private static final String PROJECT_NAME = "COVID-19";

	private static final String PROJECT_DESC = "COVID-19 Project";

	private static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/prod/";

	public static void exec(CosmosConnections conns) {
		RawDataFileUploadParams params = getParams();
		CreateProjectAction.exec(params, conns);
	}

	public static RawDataFileUploadParams getParams() {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setCreatedBy("greshje");
		params.setProjCode(PROJECT_CODE);
		params.setProjName(PROJECT_NAME);
		params.setProjDescription(PROJECT_DESC);
		params.setDatabricksFileRoot(DATABRICKS_FILE_ROOT);
		return params;
	}

}
