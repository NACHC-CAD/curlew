package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build;

import org.nachc.cad.cosmos.action.confirm.ConfirmConfiguration;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.BurnEverythingToTheGround;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.CreateProjectWomensHealth;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup.UploadRawDataFiles;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDemoGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDiagGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateEncGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateObsGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateOtherGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateProcedureGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateRxGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201121;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201122;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201123Terminology;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201124Terminology;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201127Proc;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201207MedValueSetCat;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201211Ac;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20201221He;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20210105Ochin;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20210128MedDescCatV2;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.Update20210128UpdateColumnNames;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.action.delete.DeleteProjectAction;
import org.nachc.cad.cosmos.create.valueset.Z_CreateValueSetSchema;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.util.time.Timer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Build {

	public static final String PROJECT_NAME = "womens_health";
	
	public static final String FILES_LOCATION = "/FileStore/tables/prod/womens_health";
	
	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		Timer timer = new Timer();
		timer.start();
		try {
			log("Confirming Configuration");
			ConfirmConfiguration.main(null);
			log("Adding Project");
//			DeleteProjectAction.exec(PROJECT_NAME, FILES_LOCATION, conns);
//			CreateProjectWomensHealth.createProject();
/*
			addFiles("Demographics", "demo", conns);
			addFiles("Diagnosis", "dx", conns);
			addFiles("Encounter", "enc", conns);
			addFiles("Fertility", "fert", conns);
			addFiles("Observation", "obs", conns);
			addFiles("Other", "other", conns);
			addFiles("Procedure", "proc", conns);
			addFiles("Rx", "rx", conns);
*/
			conns.commit();
			conns.close();
			log("Additional updates");

			Update20201121.main(null);
// THIS ONE HAS LOCK ISSUES
//			Update20201122.main(null);

			Update20201123Terminology.main(null);
			Update20201124Terminology.main(null);
			Update20201127Proc.main(null);
			Update20201207MedValueSetCat.main(null);
			Update20201211Ac.main(null);
			Update20201221He.main(null);

			// Update20201228AddContraceptionProject.main(null);
// THIS ONE HAS LOCK ISSUES
//			Update20210105Ochin.main(null);
			Update20210128MedDescCatV2.main(null);
			Update20210128UpdateColumnNames.main(null);
			log("Updating GROUP TABLES");
			updateGroupTables();
			log("Adding Value Sets");
			Z_CreateValueSetSchema.main(null);
			// Update99999999CreateWomensHealthSchema.main(null);
			// Update99999999GrantPrivileges.main(null);
		} finally {
			log.info("Closing connection");
			conns.close();
		}
		timer.stop();
		log.info("START:   " + timer.getStartAsString());
		log.info("DONE:    " + timer.getStopAsString());
		log.info("Elapsed: " + timer.getElapsedString());
		log.info("Done.");
	}

	private static void addFiles(String name, String abr, CosmosConnections conns) {
		log(name);
		RawDataFileUploadParams params = BuildParamsWomensHealth.getParams(name, abr);
		UploadRawDataFiles.createNewEntity(params, conns, false);
	}

	private static void updateGroupTables() {
		log("UPDATING demo GROUP TABLE (1 of 9)");
		new UpdateDemoGroupTable().doUpdate();
		log("UPDATING diag GROUP TABLE (2 of 9)");
		new UpdateDiagGroupTable().doUpdate();
		log("UPDATING enc GROUP TABLE (3 of 9)");
		new UpdateEncGroupTable().doUpdate();
//		log("UPDATING flat GROUP TABLE (4 of 9)");
//		new UpdateFlatGroupTable().doUpdate();
		log("UPDATING obs GROUP TABLE (5 of 9)");
		new UpdateObsGroupTable().doUpdate();
		log("UPDATING other GROUP TABLE (6 of 9)");
		new UpdateOtherGroupTable().doUpdate();
//		log("UPDATING preg GROUP TABLE (7 of 9)");
//		new UpdatePregGroupTable().doUpdate();
		log("UPDATING procedure GROUP TABLE (8 of 9)");
		new UpdateProcedureGroupTable().doUpdate();
		log("UPDATING rx GROUP TABLE (9 of 9)");
		new UpdateRxGroupTable().doUpdate();
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
