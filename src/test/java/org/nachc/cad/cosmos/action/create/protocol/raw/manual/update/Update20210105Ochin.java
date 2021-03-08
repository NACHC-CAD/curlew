package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup.UploadRawDataFiles;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDemoGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateDiagGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateEncGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateObsGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateProcedureGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update.UpdateRxGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.yaorma.dao.Dao;
import org.yaorma.dvo.DvoUtil;
import org.yaorma.util.time.Timer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update20210105Ochin {

	private static final String SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\update-2021-01-05-ochin\\";

	public static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/prod/womens-health/";

	public static void main(String[] args) {
		Timer timer = new Timer();
		timer.start();
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("Adding update files...");
			updateFiles("Demographics", "demo", conns);
			updateFiles("Diagnosis", "dx", conns);
			updateFiles("Encounter", "enc", conns);
			updateFiles("Lab", "lab", conns);
			updateFiles("Pregnancy", "preg", conns);
			updateFiles("Procedure", "proc", conns);
			updateFiles("Rx", "rx", conns);
			// update group table
			new UpdateDemoGroupTable().doUpdate();
			new UpdateDiagGroupTable().doUpdate();
			new UpdateEncGroupTable().doUpdate();
			new UpdateProcedureGroupTable().doUpdate();
			new UpdateRxGroupTable().doUpdate();
		} finally {
			conns.close();
		}
		timer.stop();
		log.info("START:   " + timer.getStartAsString());
		log.info("STOP:    " + timer.getStopAsString());
		log.info("ELAPSED: " + timer.getElapsedString());
		log.info("Done.");
	}

	private static void updateFiles(String name, String abr, CosmosConnections conns) {
		log(name);
		RawDataFileUploadParams params = getParams(name, abr, conns.getMySqlConnection());
		if ("obs".equals(abr)) {
			params.setDelimiter('|');
		}
		log.info("Uploading " + name + " using: " + params.getDelimiter());
		UploadRawDataFiles.updateExistingEntity(params, conns, true);
		CreateGrpDataTableAction.execute(params.getRawTableGroupCode(), conns, true);
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

	public static RawDataFileUploadParams getParams(String name, String abr, Connection mySqlConn) {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setCreatedBy("greshje");
		params.setProjCode("womens_health");
		params.setProtocolNamePretty("Women's Health");
		params.setProjCode("womens_health");
		params.setDataLot("LOT 2");
		params.setDatabricksFileLocation(DATABRICKS_FILE_ROOT + abr);
		params.setDataGroupName(name);
		params.setDataGroupAbr(abr);
		String localHostFileAbsLocation = SRC_ROOT + abr;
		params.setLocalHostFileAbsLocation(localHostFileAbsLocation);
		String code = params.getRawTableGroupCode();
		log.info("Getting raw_table_group for: " + code);
		RawTableGroupDvo rawTableGroupDvo = Dao.find(new RawTableGroupDvo(), "code", code, mySqlConn);
		if(rawTableGroupDvo == null) {
			log.info("* * * CREATING NEW RAW DATA GROUP FOR: " + code);
			rawTableGroupDvo = createNewRawTableGroup(params, code, name, mySqlConn);
			String createdGuid = rawTableGroupDvo.getGuid();
			log.info("CREATED guid: " + createdGuid);
			rawTableGroupDvo = Dao.find(new RawTableGroupDvo(), "code", code, mySqlConn);
			String foundGuid = rawTableGroupDvo.getGuid();
			log.info("FOUND guid:   " + foundGuid);
			if(createdGuid.equals(foundGuid) == false) {
				throw new RuntimeException("An error occured trying to create new data group (guids do not match)");
			}
		}
		params.setRawTableGroupDvo(rawTableGroupDvo);
		return params;
	}

	private static RawTableGroupDvo createNewRawTableGroup(RawDataFileUploadParams params, String code, String name, Connection mySqlConn) {
		RawTableGroupDvo dvo = new RawTableGroupDvo();
		CosmosDvoUtil.init(dvo, "greshje", mySqlConn);
		dvo.setProject(params.getProjCode());
		dvo.setCode(code);
		dvo.setName(params.getProtocolNamePretty() + " " + params.getDataGroupName() + " Table");
		dvo.setDescription("This table contains the raw data for " + params.getDataGroupName());
		dvo.setFileLocation(params.getDatabricksFileLocation());
		dvo.setRawTableSchema(params.getRawTableSchemaName());
		dvo.setGroupTableSchema(params.getGroupTableSchemaName());
		dvo.setGroupTableName(name);
		Dao.insert(dvo, mySqlConn);
		return dvo;
	}
	
}
