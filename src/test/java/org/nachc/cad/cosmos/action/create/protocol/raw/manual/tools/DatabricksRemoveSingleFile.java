package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import java.io.File;
import java.sql.Connection;

import org.nachc.cad.cosmos.action.delete.DeleteSingleFileAction;
import org.nachc.cad.cosmos.action.refresh.RefreshSchemaAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabricksRemoveSingleFile {

	public static final File CREATE_SCHEMA_SCRIPT = FileUtil.getFile("/org/nachc/cad/cosmos/databricks/update/projects/womenshealth/init/womens-health-tables-001.sql", true);

	private static final String SCHEMA = "womens_health";

	private static final String RAW_TABLE_SCHEMA = "prj_raw_womens_health_enc";

	private static final String RAW_TABLE_NAME = "womens_health_he_enc_health_efficient_ccdata__encounters4_1_19to3_31_20_csv";

	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		try {
			String dataGroupCode = "womens_health_proc_cat";
			log.info("Starting delete for: " + dataGroupCode);
			// get mysql connection
			log.info("Getting MySql Connection");
			Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
			// get databricks connection
			log.info("Getting Databricks Connection");
			Connection dbConn = DatabricksDbConnectionFactory.getConnection();
			// execute
			exec(conns);
			log.info("Done.");
		} finally {
			conns.close();
		}
	}

	public static void exec(CosmosConnections conns) {
		// do the delete
		DeleteSingleFileAction.delete(RAW_TABLE_SCHEMA, RAW_TABLE_NAME, conns);
		// rebuild the schema
		log.info("Rebuilding schema");
		Database.executeSqlScript(CREATE_SCHEMA_SCRIPT, conns.getDbConnection());
		// refresh schema
		log.info("Doing refresh");
		RefreshSchemaAction.exec(SCHEMA, conns.getDbConnection());
	}
	
}
