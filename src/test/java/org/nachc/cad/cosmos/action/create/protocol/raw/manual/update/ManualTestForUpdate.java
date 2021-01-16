package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.CreateIntegrationTestProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup.UploadRawDataFiles;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.action.delete.DeleteProjectAction;
import org.nachc.cad.cosmos.action.delete.DeleteRawDataGroupAction;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.dao.Dao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ManualTestForUpdate {

	private static final String SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\_integration-test\\womens-health-pp\\";

	public static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/prod/_integration-test/womens-health-pp/";

	public static void main(String[] args) {
		log.info("Starting process...");
		CosmosConnections conns = new CosmosConnections();
		try {
			delete(conns);
			exec(conns);
			conns.commit();
		} finally {
			conns.close();
		}
		log.info("Done.");
	}

	public static void delete(CosmosConnections conns) {
		log.info("Deleging demo");
		DeleteRawDataGroupAction.delete("_integration_test_wh_pp_demo", conns);
		log.info("Deleging dx");
		DeleteRawDataGroupAction.delete("_integration_test_wh_pp_dx", conns);
		log.info("Deleging enc");
		DeleteRawDataGroupAction.delete("_integration_test_wh_pp_enc", conns);
/*
		log.info("Deleging lab");
		DeleteRawDataGroupAction.delete("_integration_test_wh_pp_lab", conns);
		log.info("Deleging preg");
		DeleteRawDataGroupAction.delete("_integration_test_wh_pp_preg", conns);
		log.info("Deleging proc");
		DeleteRawDataGroupAction.delete("_integration_test_wh_pp_proc", conns);
		log.info("Deleging rx");
		DeleteRawDataGroupAction.delete("_integration_test_wh_pp_rx", conns);
*/
		log.info("Deleting project");
		DeleteProjectAction.delete("_integration_test_wh_pp", conns);
		log.info("Done with deletes");
	}

	public static void exec(CosmosConnections conns) {
		log.info("Adding project");
		// create project in mysql
		log.info("Creating project in mysql");
		CreateIntegrationTestProject.createProject(conns.getMySqlConnection());
		conns.commit();
		// upload files
		updateFiles("Demographics", "demo", conns);
//		updateFiles("Encounter", "enc", conns);
//		updateFiles("Rx", "rx", conns);
	}

	private static void updateFiles(String name, String abr, CosmosConnections conns) {
		log(name);
		RawDataFileUploadParams params = getParams(name, abr, conns.getMySqlConnection());
		UploadRawDataFiles.createNewEntity(params, conns, false);
		CreateGrpDataTableAction.execute(params.getRawTableGroupCode(), conns, true);
	}

	public static RawDataFileUploadParams getParams(String name, String abr, Connection mySqlConn) {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setCreatedBy("greshje");
		params.setProjCode("_integration_test_wh_pp");
		params.setProtocolNamePretty("Integration test, Women's Health (Post Partum)");
		params.setDataLot("LOT 2");
		params.setDatabricksFileLocation(DATABRICKS_FILE_ROOT + abr);
		params.setDataGroupName(name);
		params.setDataGroupAbr(abr);
		String localHostFileAbsLocation = SRC_ROOT + abr;
		params.setLocalHostFileAbsLocation(localHostFileAbsLocation);
		String code = params.getRawTableGroupCode();
		log.info("Getting raw_table_group for: " + code);
		RawTableGroupDvo rawTableGroupDvo = Dao.find(new RawTableGroupDvo(), "code", code, mySqlConn);
		params.setRawTableGroupDvo(rawTableGroupDvo);
		return params;
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
