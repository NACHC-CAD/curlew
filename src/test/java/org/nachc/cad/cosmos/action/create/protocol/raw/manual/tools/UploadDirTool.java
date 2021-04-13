package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.project.UploadDir;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadDirTool {

	private static final String DIR = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-04-12-COVID-SdohNameNachc\\";
	
	private static final boolean UPDATE_GROUP_TABLES = true;
	
	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		try {
			UploadDir.exec(DIR, "greshje", conns, UPDATE_GROUP_TABLES);
			conns.commit();
		} finally {
			conns.close();
		} 
		log.info("Done.");
	}
	
}
