package org.nachc.cad.cosmos.action.delete;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.yaorma.database.Data;
import org.yaorma.database.Row;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteProjectAction {

	/**
	 * 
	 * This method will delete the project database as well as the supporting prj_grp_ and prj_raw_ databases.  
	 * 
	 */
	
	public static void exec(String projectName, String filesLocation, CosmosConnections conns) {
		// check that we're not trying to delete all of prod
		if(filesLocation.endsWith("/prod/")) {
			throw new RuntimeException("PLEASE DON'T DELETE ALL OF OUR PRODUCTION FILES.");
		}
		// delete from databricks database
		log.info("DROPPING DATABASES...");
		dropDatabases(projectName, filesLocation, conns);
		// delete databricks file
		DatabricksFileUtilFactory.get().rmdir(filesLocation);
		// delete from mysql
		log.info("Doing mysql delete...");
		DeleteProjectFromMySqlAction.delete(projectName, conns);
		log.info("Done with delete.");
	}

	private static void dropDatabases(String projectName, String filesLocation, CosmosConnections conns) {
		log.info("Droping database schema: " + projectName);
 		DatabricksDbUtil.dropDatabase(projectName, conns.getDbConnection(), conns);
		Data data = DatabricksDbUtil.showSchemas(conns);
		for(Row row : data) {
			String namespace = row.get("namespace");
			if (namespace == null) {
				namespace = row.get("databasename");
			}
			if(namespace.toLowerCase().startsWith("prj_grp_" + projectName + "_") || namespace.toLowerCase().startsWith("prj_raw_" + projectName + "_")) {
				log.info("Deleting database: " + namespace);
				DatabricksDbUtil.dropDatabase(namespace, conns.getDbConnection(), conns);
			}
		}
	}
	
}
