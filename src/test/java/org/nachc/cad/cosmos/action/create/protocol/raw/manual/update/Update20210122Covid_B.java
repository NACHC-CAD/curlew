package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import java.io.File;
import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.CreateProjectCovid19;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.rawtablegroup.UploadRawDataFiles;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update20210122Covid_B {

	private static final String SRC_ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-20210122-COVID\\";

	public static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/prod/covid/";

	public static final String CREATE_DATABASE_SCRIPT = "/org/nachc/cad/cosmos/databricks/update/projects/covid/init/000-create-tables.sql";
	
	public static void main(String[] args) {
		log.info("Starting process...");
		CosmosConnections conns = new CosmosConnections();
		try {
			exec(conns);
			conns.commit();
		} finally {
			conns.close();
		}
		log.info("Done.");
	}

	public static void exec(CosmosConnections conns) {
		log.info("Adding project");
		// create project in mysql
		log.info("Creating project in mysql");
		CreateProjectCovid19.createProject(conns.getMySqlConnection());
		conns.commit();
		// upload files
		updateFiles("Admin", "admin", conns);
		updateFiles("Demographics", "demo", conns);
		updateFiles("Diagnosis", "dx", conns);
		updateFiles("Encounter", "enc", conns);
		updateFiles("Hospitalization", "hosp", conns);
		updateFiles("Labs", "lab", conns);
		updateFiles("Symptoms", "symp", conns);
		updateFiles("Tobacco", "tobacco", conns);
		updateFiles("Vitals", "vitals", conns);
		DatabricksDbUtil.dropDatabase("covid", conns.getDbConnection());
		File file = FileUtil.getFile(CREATE_DATABASE_SCRIPT, true);
		Database.executeSqlScript(file, conns.getDbConnection());
	}

	private static void updateFiles(String name, String abr, CosmosConnections conns) {
		log(name);
		RawDataFileUploadParams params = getParams(name, abr, conns.getMySqlConnection());
		UploadRawDataFiles.createNewEntity(params, conns, false);
		updateColAliases(conns);
		CreateGrpDataTableAction.execute(params.getRawTableGroupCode(), conns, true);
	}

	public static RawDataFileUploadParams getParams(String name, String abr, Connection mySqlConn) {
		RawDataFileUploadParams params = new RawDataFileUploadParams();
		params.setCreatedBy("greshje");
		params.setProjCode("covid");
		params.setProtocolNamePretty("COVID-19");
		params.setDataLot("LOT 1");
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

	private static void updateColAliases(CosmosConnections conns) {
		updatePatId(conns);
		updateEncId(conns);
		conns.commit();
	}

	private static void updatePatId(CosmosConnections conns) {
		String sqlString = "";
		sqlString += "update raw_table_col_alias \n";
		sqlString += "set col_alias = 'patient_id' \n";
		sqlString += "where 1=1 \n";
		sqlString += "	and raw_table_name like 'covid_chcn_%' \n";
		sqlString += "	and col_name = 'pat_id' \n";
		log.info("Sql: \n" + sqlString);
		Database.update(sqlString, conns.getMySqlConnection());
	}
	
	private static void updateEncId(CosmosConnections conns) {
		String sqlString = "";
		sqlString += "update raw_table_col_alias \n";
		sqlString += "set col_alias = 'encounter_id' \n";
		sqlString += "where 1=1 \n";
		sqlString += "	and raw_table_name like 'covid_chcn_%' \n";
		sqlString += "	and col_name = 'enc_id' \n";
		log.info("Sql: \n" + sqlString);
		Database.update(sqlString, conns.getMySqlConnection());
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
