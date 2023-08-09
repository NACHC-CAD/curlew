package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid;

import java.io.File;
import java.util.List;

import org.nachc.cad.cosmos.action.confirm.ConfirmConfiguration;
import org.nachc.cad.cosmos.action.create.metrics.CreateMetricsTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateBaseTablesAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.create.CreateCovidProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.delete.DeleteCovidProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.finalize.CreateCovidGroupTables;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.project.UploadDir;
import org.yaorma.database.Database;
import org.yaorma.util.time.Timer;

import com.nach.core.util.file.FileUtil;

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
		// CONFIRM CONFIGURATION
		ConfirmConfiguration.exec(conns);
		// DELETE THE EXISTING PROJECT
		DeleteCovidProject.exec(conns);
		conns.commit();
		// CREATE THE COVID PROJECT
		CreateCovidProject.exec(conns);
		conns.commit();
		// DO UPLOADS
		doUploads(conns);
		timer.stop();
		log.info("START:   " + timer.getStartAsString());
		log.info("DONE:    " + timer.getStopAsString());
		log.info("Elapsed: " + timer.getElapsedString());
		log.info("Done.");
		log.info("DONE!");
	}

	private static void doUploads(CosmosConnections conns) {
		// DO THE UPLOADS (ONLY DO ONE FOR CHCN, They are giving a full refresh)
		String root = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\";
//		uploadDir(root + "update-2021-01-22-COVID-CHCN", conns);
		uploadDir(root + "update-2021-02-07-COVID-AC", conns);
		uploadDir(root + "update-2021-02-07-COVID-HCN", conns);
		uploadDir(root + "update-2021-03-15-COVID-HCN", conns);
		uploadDir(root + "update-2021-03-15-COVID-HE", conns);
		uploadDir(root + "update-2021-03-17-COVID-LabTestCategoryNachc", conns);
		uploadDir(root + "update-2021-03-17-COVID-LabTestResultNachc", conns);
		uploadDir(root + "update-2021-03-31-COVID-AC", conns);
//		uploadDir(root + "update-2021-03-31-COVID-CHCN", conns);
		uploadDir(root + "update-2021-04-02-COVID-DemoRaceNachc", conns);
		uploadDir(root + "update-2021-04-11-COVID-DemoSexNachc", conns);
		uploadDir(root + "update-2021-04-12-COVID-SdohNameNachc", conns);
		uploadDir(root + "update-2021-04-12-COVID-SdohValueNachc", conns);
//		uploadDir(root + "update-2021-04-17-COVID-CHCN", conns);
		uploadDir(root + "update-2021-04-19-COVID-AC", conns);
		uploadDir(root + "update-2021-04-29-COVID-HE", conns);
		uploadDir(root + "update-2021-04-30-COVID-VaccCategoryNachc", conns);
		uploadDir(root + "update-2021-04-30-COVID-zipcode", conns);
		uploadDir(root + "update-2021-05-19-COVID-HE", conns);
		uploadDir(root + "update-2021-05-27-COVID-CHCN", conns);
		uploadDir(root + "update-2021-05-27-COVID-HCN", conns);
		uploadDir(root + "update-2021-05-29-COVID-VaccCategoryNachc", conns);
		uploadDir(root + "update-2021-06-03-COVID-AC", conns);
		uploadDir(root + "update-2021-06-13-COVID-VaccCategoryNachc", conns);
		uploadDir(root + "update-2021-06-14-COVID-HE", conns);
		uploadDir(root + "update-2021-06-18-COVID-CHCN", conns);
		uploadDir(root + "update-2021-06-19-COVID-months", conns);
		uploadDir(root + "update-2021-06-22-COVID-APCA", conns);
		// CREATE THE GROUP TABLES
		CreateCovidGroupTables.exec(conns);
		conns.commit();
		CreateBaseTablesAction.exec("covid");
		conns.commit();
		CreateMetricsTable.exec("covid", "covid_metrics", conns);
		conns.commit();
		// RUN THE SCRIPTS TO CREATE THE OTHER SCHEMAS
		// runScripts(conns);
	}
	
	private static void uploadDir(String dirName, CosmosConnections conns) {
		log("UPLODAING DIR: " + dirName);
		UploadDir.exec(dirName, "greshje", conns, false);
		conns.commit();
	}

	private static void runScripts(CosmosConnections conns) {
		File dir = FileUtil.getFromProjectRoot("etc/resources/etl/covid/covid-etl-scripts");
		List<File> files = FileUtil.listFiles(dir,"*.sql");
		log.info("Got " + files.size() + " scripts to run");
		for(File file : files) {
			log("RUNNING SCRIPT FOR: " + file.getName());
			Database.executeSqlScript(file, conns.getDbConnection());
		}
		log("DONE RUNNING SCRIPTS");
	}
	
	//
	// main method (see exec method below for implementation)
	//

	public static void main(String[] args) {
		log.info("Starting main...");
		log.info("Getting connection...");
		CosmosConnections conns = CosmosConnections.getConnections();
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
		str += "\n\n";
		str += "-- -------------------------------------------------------------\n";
		str += "-- * * * \n";
		str += "-- * \n";
		str += "-- * " + msg + "\n";
		str += "-- * \n";
		str += "-- * * * \n";
		str += "-- -------------------------------------------------------------\n";
		log.info(str);
	}

}
