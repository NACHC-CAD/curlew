package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import java.io.File;
import java.sql.Connection;

import org.nachc.cad.cosmos.action.delete.DeleteSingleFileAction;
import org.nachc.cad.cosmos.action.refresh.RefreshSchemaAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.database.Database;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update20201221RemoveHe {

	public static final File CREATE_SCHEMA_SCRIPT = FileUtil.getFile("/org/nachc/cad/cosmos/databricks/update/projects/womenshealth/init/womens-health-tables-001.sql", true);

	private static final String SCHEMA = "womens_health";

	public static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/prod/womens-health/";

	public static void main(String[] args) {
		CosmosConnections conns = CosmosConnections.getConnections();
		try {
			// get connections
			log.info("Getting mysql connection...");
			Connection mySqlConn = conns.getMySqlConnection();
			log.info("Getting databricks connection...");
			Connection dbConn = conns.getDbConnection();
			log.info("Got connections");
			// do removes
			String rawTableSchema;
			String rawTableName;
			// enc
			log.info("Deleting enc");
			rawTableSchema = "prj_raw_womens_health_enc";
			rawTableName = "womens_health_he_enc_health_efficient_ccdata__encounters4_1_19to3_31_20_csv";
			DeleteSingleFileAction.delete(rawTableSchema, rawTableName, conns);
			// demo
			log.info("Deleting demo");
			rawTableSchema = "prj_raw_womens_health_demo";
			rawTableName = "womens_health_he_demo_health_efficient_ccdata__demographics4_1_19to3_31_20_csv";
			DeleteSingleFileAction.delete(rawTableSchema, rawTableName, conns);
			// rx
			log.info("Deleting rx");
			rawTableSchema = "prj_raw_womens_health_rx";
			rawTableName = "womens_health_he_rx_health_efficient_ccdata__medications4_1_19to3_31_20_csv";
			DeleteSingleFileAction.delete(rawTableSchema, rawTableName, conns);
		} finally {
			conns.close();
		}
		log.info("Done.");
	}

}
