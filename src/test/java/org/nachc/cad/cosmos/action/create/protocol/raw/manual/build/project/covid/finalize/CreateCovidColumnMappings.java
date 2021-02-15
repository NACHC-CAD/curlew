package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.finalize;

import java.io.File;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateColumnMappingsAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateCovidColumnMappings {

	public static final String FILE_NAME = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-20210207-COVID-AC\\_meta\\ac-files-meta-data.xlsx";

	public static void exec(CosmosConnections conns) {
		log.info("Creating column aliases...");
		File srcFile = new File(FILE_NAME);
		CreateColumnMappingsAction.exec(srcFile, conns);
		log.info("Done creating column aliases.");
	}

}
