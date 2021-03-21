package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.update;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.project.UploadDir;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateDir_20210315_Covid_HE {

	private static final String DIR = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-20210315-HE-COVID-DATA";

	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		try {
			UploadDir.exec(DIR, "greshje", conns);
			conns.commit();
			log.info("Done.");
		} finally {
			conns.close();
		}
	}

}
