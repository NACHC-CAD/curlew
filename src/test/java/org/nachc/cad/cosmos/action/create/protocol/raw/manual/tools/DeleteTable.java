package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.delete.DeleteRawDataGroupAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 * Deletes all of the artifacts for a single Databricks table (raw_data_group_code).  
 * Use this class to delete an entire data group (e.g. the covid enc table)
 *
 */
@Slf4j
public class DeleteTable {

	private static final String GROUP_CODE = "covid_sdoh_name_nachc";
	
	public static void main(String[] args) {
		CosmosConnections conns = CosmosConnections.getConnections();
		try {
			log.info("Starting delete for: " + GROUP_CODE);
			log.info("Getting connections...");
			log.info("Doing delete...");
			DeleteRawDataGroupAction.delete(GROUP_CODE, conns);
		} finally {
			conns.close();
		}
		log.info("Done.");
	}

}
