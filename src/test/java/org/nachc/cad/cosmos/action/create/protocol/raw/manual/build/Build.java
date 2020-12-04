package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build;

import org.nachc.cad.cosmos.action.create.protocol.raw.manual.BurnEverythingToTheGround;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.CreateProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup.UploadRawDataFiles;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDemoGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDiagGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateEncGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateRxGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201121;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201122;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201123Terminology;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201124Terminology;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201127Proc;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.create.valueset.Z_CreateValueSetSchema;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Build {

	public static void main(String[] args) {
		log("Burning everything to the ground.");
		BurnEverythingToTheGround.main(null);
		log("Adding Project");
		CreateProject.createProject();
		addFiles("Demographics", "demo");
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
		log("Addtional updates");
		Update20201121.main(null);
		Update20201122.main(null);
		Update20201123Terminology.main(null);
		Update20201124Terminology.main(null);
		Update20201127Proc.main(null);
		log("Adding Value Sets");
		Z_CreateValueSetSchema.main(null);
		log.info("Done.");
	}

	private static void addFiles(String name, String abr) {
		log(name);
		RawDataFileUploadParams params = BuildParams.getParams(name, abr);
		UploadRawDataFiles.createNewEntity(params, false);
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
