package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.project.UploadDir;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadDirTool {

	public static final String DIR = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-20210317-COVID-LabTestResultNachc";
	
	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		try {
			UploadDir.exec(DIR, "greshje", conns);
			conns.commit();
		} finally {
			conns.close();
		}
		log.info("Done.");
	}
	
}
