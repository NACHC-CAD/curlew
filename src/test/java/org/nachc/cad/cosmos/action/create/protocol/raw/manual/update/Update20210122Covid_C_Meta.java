package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import org.nachc.cad.cosmos.action.create.metrics.CreateMetricsTable;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update20210122Covid_C_Meta {

	public static void main(String[] args) {
		log.info("Starting...");
		log.info("Getting connections");
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("Dropping meta schema");
			DatabricksDbUtil.dropDatabase("covid_meta", conns.getDbConnection());
			log.info("Creating schema");
			DatabricksDbUtil.createDatabase("covid_meta", conns.getDbConnection());
			log.info("Creating meta data tables");
			CreateMetricsTable.createMetaSchema("covid", "covid_meta", conns);
		} finally {
			conns.close();
		}
		log.info("Done.");
	}
	
}
