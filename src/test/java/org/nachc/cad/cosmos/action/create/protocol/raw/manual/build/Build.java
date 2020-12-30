package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build;

import org.nachc.cad.cosmos.action.create.protocol.raw.manual.BurnEverythingToTheGround;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.CreateProjectWomensHealth;
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
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201207MedValueSetCat;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201211Ac;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201221He;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201221RemoveHe;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.create.valueset.Z_CreateValueSetSchema;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Build {

	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
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
			Update20201207MedValueSetCat.main(null);
			Update20201211Ac.main(null);
			Update20201221He.main(null);
			Update20201221RemoveHe.main(null);
			log("Adding Value Sets");
			Z_CreateValueSetSchema.main(null);
			log.info("Done.");
		} finally {
			conns.close();
		}
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
