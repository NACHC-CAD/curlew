package org.nachc.cad.cosmos.action.create.metrics;

import org.junit.Test;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateMetricsTableManualTest {

	@Test
	public void shouldGetSqlString() {
		log.info("Starting test...");
		log.info("Getting connections...");
		CosmosConnections conns = new CosmosConnections();
		String tableSchema = "covid";
		String tableName = "admin";
		log.info("Getting sqlString");
		String sqlString = CreateMetricsTable.getMetricsTableSqlString(tableSchema, tableName, conns);
		log.info("Got sqlString: \n" + sqlString);
		log.info("Done.");
	}
	
}
