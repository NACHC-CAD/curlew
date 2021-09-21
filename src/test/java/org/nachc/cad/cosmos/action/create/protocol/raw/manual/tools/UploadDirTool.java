package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import java.io.File;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateBaseTablesAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.project.UploadDir;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadDirTool {

	private static final String[] DIRS = {
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\standard-values"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-08-06-COVID-HE"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-08-07-COVID-InsuranceDi"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-08-07-COVID-fpl"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid-public\\covid-cdc-case-surveillance-2021-08-08"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-07-22-COVID-AC"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-07-21-COVID-HCN"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-07-13-COVID-CHCN"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\ac\\update-2021-06-17-COVID-AC"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\hcn\\update-2021-06-25-COVID-HCN"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\omop-concepts\\update-NSV-OMOP-Concepts-2021-08-17"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\nachc-term\\update-2021-08-15-NACHC_TERM-months",
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\nachc-term\\update-2021-04-02-COVID-DemoRaceNachc",
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\nachc-term\\update-2021-04-11-COVID-DemoSexNachc"
			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\he\\update-2021-09-20-COVID-HE"			
 			// DON'T FORGET TO CHANGE PROJECT CODE
	};

	// DON'T FORGET TO CHANGE PROJECT CODE
	private static final String PROJ = "covid";

	private static final boolean UPDATE_BASE_TABLES = true;

	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		try {
			for (String dir : DIRS) {
				log.info("\n\n\n-----------------------------------------------------------\n");
				log.info("* * * DOING UPLOAD FOR: " + dir);
				// upload the file
				UploadDir.exec(dir, "greshje", conns, false);
				conns.commit();
			}
			if (UPDATE_BASE_TABLES == true) {
				// create base tables
				CreateBaseTablesAction.exec(PROJ, conns);
				conns.commit();
			}
		} finally {
			conns.close();
		}
		log.info("Done.");
	}

}
