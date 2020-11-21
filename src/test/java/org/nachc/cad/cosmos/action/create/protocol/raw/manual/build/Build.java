package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build;

import org.nachc.cad.cosmos.action.create.protocol.raw.manual.BurnEverythingToTheGround;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.CreateProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup.CreateNewRawTableGroupFromLocalFiles;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDemoGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDiagGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateEncGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateRxGroupTable;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Build {

	public static void main(String[] args) {
		log("Burning everything to the ground.");
		BurnEverythingToTheGround.main(null);
		log("Adding Project");
		CreateProject.createProject();
		addFiles("Demo", "demo");
		addFiles("Diagnosis", "dx");
		addFiles("Encounter", "enc");
		addFiles("Fertility", "fert");
		addFiles("Observation", "obs");
		addFiles("Other", "other");
		addFiles("Procedure", "proc");
		addFiles("Rx", "rx");
		log("Doing updates");
		new UpdateDemoGroupTable().doUpdate();
		new UpdateDiagGroupTable().doUpdate();
		new UpdateEncGroupTable().doUpdate();
		new UpdateRxGroupTable().doUpdate();
		log.info("Done.");
	}

	private static void addFiles(String name, String abr) {
		log(name);
		CreateNewRawTableGroupFromLocalFiles.exec(BuildParams.getParams(name, abr));
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
