package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.action.create.metrics.CreateMetricsByOrgTable;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabricksCreateMetricsByOrgSchema {

	public static final String SRC_SCHEMA = "covid_bronze_pat_star";
	
	public static final String DST_SCHEMA = "covid_bronze_pat_star_metrics_by_org";
	
	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		try {
			CreateMetricsByOrgTable.exec(SRC_SCHEMA, DST_SCHEMA, conns);
		} finally {
			conns.close();
		}
		log.info("Done.");
	}
	
}
