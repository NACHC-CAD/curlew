package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.delete.DeleteRawDataGroupAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabricksRemoveRawDataGroup {

	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		try {
			String dataGroupCode = "womens_health_proc_cat";
			log.info("Starting delete for: " + dataGroupCode);
			log.info("Getting connections...");
			log.info("Doing delete...");
			DeleteRawDataGroupAction.delete(dataGroupCode, conns);
			log.info("Done.");
		} finally {
			conns.close();
		}
	}

}
