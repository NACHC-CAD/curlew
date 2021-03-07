package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.update;

import org.nachc.cad.cosmos.action.create.project.UploadFilesAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.create.CreateCovidProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Updateupdate_20210306_NACHC_Mappings {

	private static final String SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-20210306-NACHC-Mappings\\";

	public static void exec(CosmosConnections conns) {
		RawDataFileUploadParams params = CreateCovidProject.getParams();
		params.setLocalHostFileRootDir(SRC_ROOT);
		UploadFilesAction.exec("sdoh_observation_name_nachc", "sdoh_observation_name_nachc", "LOT 1", params, conns);
		UploadFilesAction.exec("sdoh_observation_value_nachc", "sdoh_observation_value_nachc", "LOT 1", params, conns);
	}

}
