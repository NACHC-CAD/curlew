package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.delete;

import org.nachc.cad.cosmos.action.delete.DeleteProjectAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

public class DeleteCovidProject {

	private static final String DATABRICKS_FILE_ROOT = "/FileStore/tables/prod/";

	public static void exec(CosmosConnections conns) {
		DeleteProjectAction.exec("covid", DATABRICKS_FILE_ROOT + "covid", conns);
		DeleteProjectAction.exec("covid_meta", DATABRICKS_FILE_ROOT + "covid_meta", conns);
		DeleteProjectAction.exec("covidcovid", DATABRICKS_FILE_ROOT + "covid_loinc", conns);
	}

}
