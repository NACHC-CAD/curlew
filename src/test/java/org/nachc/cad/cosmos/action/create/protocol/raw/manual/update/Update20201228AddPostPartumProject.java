package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.CreateProjectWomensHealthPostPartum;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup.UploadRawDataFiles;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update20201228AddPostPartumProject {

	private static final String SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\update-2020-12-21-HE\\";

	public static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/prod/womens-health-pp/";

	public static void main(String[] args) {
		log.info("Adding project");
		// get the mysql connection
		log.info("Getting mySql Connection");
		Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
		// get the databricks connection
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		// create project in mysql
		log.info("Creating project in mysql");
		// CreateProjectWomensHealthPostPartum.createProject(mySqlConn);
		Database.commit(mySqlConn);
		// upload files
		updateFiles("Demographics", "demo", mySqlConn, dbConn);
		updateFiles("Encounter", "enc", mySqlConn, dbConn);
		updateFiles("Rx", "rx", mySqlConn, dbConn);
		// done
		log.info("Done");
	}

	private static void updateFiles(String name, String abr, Connection mySqlConn, Connection dbConn) {
		log(name);
		RawDataFileUploadParams params = getParams(name, abr, mySqlConn);
		UploadRawDataFiles.createNewEntity(params, false);
		CreateGrpDataTableAction.execute(params.getRawTableGroupCode(), dbConn, mySqlConn, true);
	}

	public static RawDataFileUploadParams getParams(String name, String abr, Connection mySqlConn) {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setCreatedBy("greshje");
		params.setProjCode("womens_health_pp");
		params.setProtocolNamePretty("Women's Health Post Partum");
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
