package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

// JEG: DELETE THIS CLASS

@Slf4j
public class CreateProjectLoinc {

	private static final String CODE = "covid_loinc";

	private static final String NAME = "COVID-19 Loinc";

	private static final String DESC = "Loinc Codes provided for the COVID-19 project";

	private static void createProject(CosmosConnections conns) {
		// CreateProjectCodesAction.exec(CODE, NAME, DESC, conns);
	}

}
