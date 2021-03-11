package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.update;

import org.nachc.cad.cosmos.action.create.project.UploadFilesAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.create.CreateCovidProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update20210122_Covid_CHCN {

	private static final String SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-20210122-COVID-CHCN\\";

	public static void exec(CosmosConnections conns) {
		RawDataFileUploadParams params = CreateCovidProject.getParams();
		params.setLocalHostFileAbsLocation(SRC_ROOT);
		UploadFilesAction.exec("Administration", "admin", "LOT 1", params, conns);
		UploadFilesAction.exec("Demographics", "demo", "LOT 1", params, conns);
		UploadFilesAction.exec("Diagnosis", "dx", "LOT 1", params, conns);
		UploadFilesAction.exec("Encounter", "enc", "LOT 1", params, conns);
		UploadFilesAction.exec("Hospitalization", "hosp", "LOT 1", params, conns);
		UploadFilesAction.exec("Labs", "lab", "LOT 1", params, conns);
		UploadFilesAction.exec("Symptoms", "symp", "LOT 1", params, conns);
		UploadFilesAction.exec("Tobacco", "tobacco", "LOT 1", params, conns);
		UploadFilesAction.exec("Vitals", "vitals", "LOT 1", params, conns);
	}

}
