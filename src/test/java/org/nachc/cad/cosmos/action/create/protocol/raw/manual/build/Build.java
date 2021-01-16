package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build;

import org.nachc.cad.cosmos.action.create.protocol.raw.manual.BurnEverythingToTheGround;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.CreateProjectWomensHealth;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup.UploadRawDataFiles;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201121;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201122;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201123Terminology;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201124Terminology;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201127Proc;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201207MedValueSetCat;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201211Ac;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201221He;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201221RemoveHe;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201228AddContraceptionProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update99999999CreateWomensHealthSchema;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.create.valueset.Z_CreateValueSetSchema;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.util.time.Timer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Build {

	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		Timer timer = new Timer();
		timer.start();
		try {
			log("Burning everything to the ground.");
			BurnEverythingToTheGround.main(null);
			log("Adding Project");
			CreateProjectWomensHealth.createProject();
			addFiles("Demographics", "demo", conns);
			addFiles("Diagnosis", "dx", conns);
			addFiles("Encounter", "enc", conns);
			addFiles("Fertility", "fert", conns);
			addFiles("Observation", "obs", conns);
			addFiles("Other", "other", conns);
			addFiles("Procedure", "proc", conns);
			addFiles("Rx", "rx", conns);
			log("Addtional updates");
			Update20201121.main(null);
			Update20201122.main(null);
			Update20201123Terminology.main(null);
			Update20201124Terminology.main(null);
			Update20201127Proc.main(null);
			Update20201207MedValueSetCat.main(null);
			Update20201211Ac.main(null);
			Update20201221He.main(null);
			Update20201228AddContraceptionProject.main(null);
			log("Adding Value Sets");
			Z_CreateValueSetSchema.main(null);
//			Update99999999CreateWomensHealthSchema.main(null);
		} finally {
			log.info("Closing connection");
			conns.close();
		}
		timer.stop();
		log.info("START:   " + timer.getStart());
		log.info("DONE:    " + timer.getStop());
		log.info("Elapsed: " + timer.getElapsedString());
		log.info("Done.");
	}

	private static void addFiles(String name, String abr, CosmosConnections conns) {
		log(name);
		RawDataFileUploadParams params = BuildParamsWomensHealth.getParams(name, abr);
		UploadRawDataFiles.createNewEntity(params, conns, false);
	}

	private static void log(String msg) {
		String logMsg = "\n\n\n\n\n";
		logMsg += "* ----------------------------------------------------------------- \n";
		logMsg += "*\n";
		logMsg += "* " + msg + "\n";
		logMsg += "*\n";
		logMsg += "* ----------------------------------------------------------------- \n";
		log.info(logMsg);
	}

}
