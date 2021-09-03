package org.nachc.cad.cosmos.util.connection;

import org.junit.Test;
import org.yaorma.database.Data;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CosmosConnectionResetIntegrationTest {

	@Test
	public void connectionShouldWorkAfterReset() {
		log.info("Starting test...");
		CosmosConnections conns = new CosmosConnections();
		// show databases
		Data data;
		data = DatabricksDbUtil.showSchemas(conns);
		log.info("Got " + data.size() + " databases.");
		// close the connections
		conns.close();
		// reset connection
		log.info("DOING RESET...");
		conns.resetConnections();
		data = DatabricksDbUtil.showSchemas(conns);
		log.info("Got " + data.size() + " databases.");
		log.info("Done.");
	}
	
}
