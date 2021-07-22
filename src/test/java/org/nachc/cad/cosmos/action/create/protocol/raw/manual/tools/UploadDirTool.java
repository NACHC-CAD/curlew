package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateBaseTablesAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.project.UploadDir;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadDirTool {

	private static final String[] DIRS = {
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-06-14-COVID-HE\\"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-06-18-COVID-CHCN\\"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-06-22-COVID-APCA\\"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-06-23-COVID-VaccCategoryNachc\\"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-03-17-COVID-LabTestCategoryNachc\\"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-07-01-COVID-HE\\"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-07-07-COVID-LPCA\\"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-07-16-COVID-HE"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-07-21-COVID-HE\\"
//			"C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-07-21-COVID-APCA\\"
			"C:\\_WORKSPACES\\_COSMOS_SERVER\\_UPLOAD\\test-HIV-ELR-THUMB2021-07-22_000\\test-HIV-ELR-THUMB"
	};

	private static final boolean UPDATE_GROUP_TABLES = false;
	
	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		try {
			for (String dir : DIRS) {
				log.info("\n\n\n-----------------------------------------------------------\n");
				log.info("* * * DOING UPLOAD FOR: " + dir);
				UploadDir.exec(dir, "greshje", conns, UPDATE_GROUP_TABLES);
				conns.commit();
			}
			if(UPDATE_GROUP_TABLES == true) {
				CreateBaseTablesAction.exec("covid", conns);
				conns.commit();
			}
		} finally {
			conns.close();
		} 
		log.info("Done.");
	}
	
}
