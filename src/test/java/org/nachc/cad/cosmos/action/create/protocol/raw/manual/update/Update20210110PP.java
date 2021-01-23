package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.CreateProjectWomensHealthPostPartum;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup.UploadRawDataFiles;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDemoGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDiagGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateEncGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateObsGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateOtherGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdatePregGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateProcedureGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateRxGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.action.delete.DeleteProjectFromMySqlAction;
import org.nachc.cad.cosmos.action.delete.DeleteRawDataGroupAction;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.dao.Dao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update20210110PP {

	private static final String SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\update-2021-01-10-PP\\";

	public static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/prod/womens-health-pp/";

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
		DeleteRawDataGroupAction.delete("womens_health_pp_demo", conns);
		log.info("Deleging dx");
		DeleteRawDataGroupAction.delete("womens_health_pp_dx", conns);
		log.info("Deleging enc");
		DeleteRawDataGroupAction.delete("womens_health_pp_enc", conns);
		log.info("Deleging lab");
		DeleteRawDataGroupAction.delete("womens_health_pp_lab", conns);
		log.info("Deleging obs");
		DeleteRawDataGroupAction.delete("womens_health_pp_obs", conns);
		log.info("Deleging preg");
		DeleteRawDataGroupAction.delete("womens_health_pp_preg", conns);
		log.info("Deleging proc");
		DeleteRawDataGroupAction.delete("womens_health_pp_proc", conns);
		log.info("Deleging proc");
		DeleteRawDataGroupAction.delete("womens_health_pp_proc_cat", conns);
		log.info("Deleging rx");
		DeleteRawDataGroupAction.delete("womens_health_pp_rx", conns);
		// delete project
		log.info("Deleting project");
		DeleteProjectFromMySqlAction.delete("womens_health_pp", conns);
		conns.commit();
		log.info("Done with deletes");
	}

	public static void exec(CosmosConnections conns) {
		log.info("Adding project");
		// create project in mysql
		log.info("Creating project in mysql");
		CreateProjectWomensHealthPostPartum.createProject(conns.getMySqlConnection());
		conns.commit();
		// upload files
		updateFiles("Demographics", "demo", conns);
		updateFiles("Dx", "dx", conns);
		updateFiles("Encounter", "enc", conns);
		updateFiles("Labs", "lab", conns);
		updateFiles("Obs", "obs", conns);
		updateFiles("Pregnancy", "preg", conns);
		updateFiles("Procedure", "proc", conns);
		updateFiles("Rx", "rx", conns);
	}

	private static void updateFiles(String name, String abr, CosmosConnections conns) {
		log(name);
		RawDataFileUploadParams params = getParams(name, abr, conns.getMySqlConnection());
		UploadRawDataFiles.createNewEntity(params, conns, false);
		log.info("Doing col updates.");
		UpdateDemoGroupTable.updateColumnAliaises(conns.getMySqlConnection());
		UpdateDiagGroupTable.updateColumnAliaises(conns.getMySqlConnection());
		UpdateEncGroupTable.updateColumnAliaises(conns.getMySqlConnection());
		UpdateObsGroupTable.updateColumnAliaises(conns.getMySqlConnection());
		UpdateOtherGroupTable.updateColumnAliaises(conns.getMySqlConnection());
		UpdateProcedureGroupTable.updateColumnAliaises(conns.getMySqlConnection());
		UpdateRxGroupTable.updateColumnAliaises(conns.getMySqlConnection());
		UpdatePregGroupTable.updateColumnAliaises(conns.getMySqlConnection());
		conns.commit();
		log.info("Done with col updates");
		CreateGrpDataTableAction.execute(params.getRawTableGroupCode(), conns, true);
	}

	public static RawDataFileUploadParams getParams(String name, String abr, Connection mySqlConn) {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setCreatedBy("greshje");
		params.setProjCode("womens_health_pp");
		params.setProtocolNamePretty("Women's Health (Post Partum)");
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
