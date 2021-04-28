package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.action.create.metrics.CreateMetricsTable;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabricksCreateMetricsSchema {

	public static final String SRC_SCHEMA = "womens_health";
	
	public static final String DST_SCHEMA = "womens_health_metrics";
	
	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		try {
			CreateMetricsTable.exec(SRC_SCHEMA, DST_SCHEMA, conns);
		} finally {
			conns.close();
		}
		log.info("Done.");
	}
	
}
