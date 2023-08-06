package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.update;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.project.UploadDir;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateDir_20210320_Covid_NACHC_CovidTestNameMap {

	private static final String DIR = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-20210320-COVID-CovidTestNameMap";

	public static void main(String[] args) {
		CosmosConnections conns = CosmosConnections.getConnections();
		try {
			UploadDir.exec(DIR, "greshje", conns);
			conns.commit();
			log.info("Done.");
		} finally {
			conns.close();
		}
	}

}
