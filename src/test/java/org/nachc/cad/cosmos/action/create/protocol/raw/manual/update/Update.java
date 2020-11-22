package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup.UploadRawDataFiles;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDemoGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateEncGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateOtherGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateProcedureGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateRxGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;

import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update {

	public static void main(String[] args) {
		log.info("Adding update files...");
		updateFiles("Demographics", "demo");
		updateFiles("Encounter", "enc");
		updateFiles("Other", "other");
		updateFiles("Procedure", "proc");
		updateFiles("Rx", "rx");
		log("Doing updates");
		new UpdateDemoGroupTable().doUpdate();
		new UpdateEncGroupTable().doUpdate();
		new UpdateOtherGroupTable().doUpdate();
		new UpdateProcedureGroupTable().doUpdate();
		new UpdateRxGroupTable().doUpdate();
		log.info("Done.");
	}

	private static void updateFiles(String name, String abr) {
		log(name);
		RawDataFileUploadParams params = UpdateParams.getParams(name, abr);
		UploadRawDataFiles.updateExistingEntity(params, true);
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
