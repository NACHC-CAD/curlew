package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.update;

import org.nachc.cad.cosmos.action.create.project.UploadFilesAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.create.CreateCovidProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update20210121_Covid_Loinc {

	private static final String SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid_loinc\\update-2021-03-20-TEST\\";

	public static void exec(CosmosConnections conns) {
		RawDataFileUploadParams params = CreateCovidProject.getParams();
		params.setLocalHostFileAbsLocation(SRC_ROOT);
		UploadFilesAction.exec("nachc_loinc_codes", "nachc_loinc_codes", "LOT 1", params, conns);
	}

	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		try { 
			exec(conns);
			conns.commit();
		} finally {
			conns.close();
		}
	}
	
}
