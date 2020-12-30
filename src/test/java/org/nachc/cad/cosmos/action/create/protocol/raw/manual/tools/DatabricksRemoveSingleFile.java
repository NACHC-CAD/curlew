package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.delete.DeleteSingleFileAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabricksRemoveSingleFile {

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
		DeleteSingleFileAction.delete(RAW_TABLE_SCHEMA, RAW_TABLE_NAME, conns);
	}

}
