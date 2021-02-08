package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.finalize;

import java.io.File;

import org.nachc.cad.cosmos.action.create.metrics.CreateMetricsTable;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateCovidSchemas {

	public static final String FILE_NAME = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\databricks-sql\\000-covid-create-tables.sql";
	
	public static void exec(CosmosConnections conns) {
		log.info("Dropping COVID database");
		DatabricksDbUtil.dropDatabase("covid", conns.getDbConnection(), conns);
		log.info("Creating covid schema");
		File file = new File(FILE_NAME);
		Database.executeSqlScript(file, conns.getDbConnection());
		log.info("Dropping meta schema");
		DatabricksDbUtil.dropDatabase("covid_meta", conns.getDbConnection(), conns);
		log.info("Creating schema");
		DatabricksDbUtil.createDatabase("covid_meta", conns.getDbConnection());
		log.info("Creating meta data tables");
		CreateMetricsTable.createMetaSchema("covid", "covid_meta", conns);
		log.info("Done creating meta.");
	}

}
