package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid;

import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.create.CreateCovidProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.delete.DeleteCovidProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.finalize.CreateCovidGroupTables;
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
		// DELETE THE EXISTING PROJECT
		DeleteCovidProject.exec(conns);
		conns.commit();
		// CREATE THE COVID PROJECT
		CreateCovidProject.exec(conns);
		conns.commit();
		// DO THE UPLOADS (ONLY DO ONE FOR CHCN, They are giving a full refresh)
		String root = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\";
		uploadDir(root + "update-2021-01-22-COVID-CHCN", conns);
		uploadDir(root + "update-2021-02-07-COVID-AC", conns);
		uploadDir(root + "update-2021-02-07-COVID-HCN", conns);
		uploadDir(root + "update-2021-03-15-COVID-HCN", conns);
		uploadDir(root + "update-2021-03-15-COVID-HE", conns);
		uploadDir(root + "update-2021-03-17-COVID-LabTestResultNachc", conns);
		uploadDir(root + "update-2021-03-31-COVID-AC", conns);
		uploadDir(root + "update-2021-03-31-COVID-CHCN", conns);
		uploadDir(root + "update-2021-04-02-COVID-RaceNachc", conns);
		// CREATE THE GROUP TABLES
		CreateCovidGroupTables.exec(conns);
		conns.commit();
		conns.commit();
		timer.stop();
		log.info("START:   " + timer.getStartAsString());
		log.info("DONE:    " + timer.getStopAsString());
		log.info("Elapsed: " + timer.getElapsedString());
		log.info("Done.");
		log.info("DONE!");
	}

	private static void uploadDir(String dirName, CosmosConnections conns) {
		log("UPLODAING DIR: " + dirName);
		UploadDir.exec(dirName, "greshje", conns, false);
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

	private static void log(String msg) {
		String str = "";
		str += "Building COVID...\n";
		str += "---------------------------------------------------------------";
		str += "\n\n\n\n";
		str += "-- * * * \n";
		str += "-- \n";
		str += "-- " + msg + "\n";
		str += "-- \n";
		str += "-- * * * \n\n";
		log.info(str);
	}

}
