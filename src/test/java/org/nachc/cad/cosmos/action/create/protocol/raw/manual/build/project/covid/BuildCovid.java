package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid;

import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.create.CreateCovidProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.delete.DeleteCovidProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.finalize.CreateCovidColumnMappings;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.finalize.CreateCovidGroupTables;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.update.Update20210207_Covid_AC;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.update.Update20210207_Covid_HCN;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.project.UploadDir;
import org.yaorma.util.time.Timer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BuildCovid {

	/**
	 * 
	 * This method will delete and rebuild the COVID-19 projects: (covid,
	 * covid_meta, covid_loinc)
	 * 
	 */

	public static void exec(CosmosConnections conns) {
		Timer timer = new Timer();
		timer.start();
		// delete and recreate the project
		DeleteCovidProject.exec(conns);
		conns.commit();
		CreateCovidProject.exec(conns);
		conns.commit();
		// do the legacy file uploads
//		Update20210122_Covid_CHCN.exec(conns);
		conns.commit();
		Update20210207_Covid_AC.exec(conns);
		conns.commit();
		Update20210207_Covid_HCN.exec(conns);
		conns.commit();
		// create the database objects
		CreateCovidColumnMappings.exec(conns);
		conns.commit();
		CreateCovidGroupTables.exec(conns);
		conns.commit();
		// do the new uploads
		// partner data
		uploadDir("C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-20210315-HCN-COVID-DATA", conns);
		uploadDir("C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-20210315-HE-COVID-DATA", conns);
		// mappings
		uploadDir("C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-20210317-COVID-LabTestResultNachc", conns);
		uploadDir("C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-20210331\\ac-2021-03-31", conns);
		uploadDir("C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-20210331\\chcn-2021-03-31", conns);
		conns.commit();
		timer.stop();
		log.info("START:   " + timer.getStartAsString());
		log.info("DONE:    " + timer.getStopAsString());
		log.info("Elapsed: " + timer.getElapsedString());
		log.info("Done.");
		log.info("DONE!");
	}

	private static void uploadDir(String dirName, CosmosConnections conns) {
		UploadDir.exec(dirName, "greshje", conns);
		conns.commit();
	}

	//
	// main method (see exec method below for implementation)
	//

	public static void main(String[] args) {
		log.info("Starting main...");
		log.info("Getting connection...");
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("executing...");
			exec(conns);
			log.info("Committing");
			conns.commit();
			log.info("Done with commit");
		} finally {
			log.info("Closing connections...");
			conns.close();
			log.info("Done with close");
		}
		log.info("Done.");
	}

}
