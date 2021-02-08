package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.action.confirm.ConfirmConfiguration;
import org.nachc.cad.cosmos.action.delete.DeleteProjectFromMySqlAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteProject {

	public static final String DBNAME = "million_hearts";

	public static void main(String[] args) {
		log.info("Confirming Configuration...");
		ConfirmConfiguration.main(null);
		log.info("Getting connection...");
		CosmosConnections conns = new CosmosConnections();
		log.info("Droping database schema: " + DBNAME);
		DatabricksDbUtil.dropDatabase(DBNAME, conns.getDbConnection(), conns);
		log.info("Doing mysql delete...");
		DeleteProjectFromMySqlAction.delete(DBNAME, conns);
		conns.commit();
		log.info("Done.");
	}

}
