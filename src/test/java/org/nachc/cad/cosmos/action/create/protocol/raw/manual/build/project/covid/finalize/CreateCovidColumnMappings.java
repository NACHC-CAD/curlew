package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid.finalize;

import java.io.File;
import java.util.List;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateColumnMappingsAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateCovidColumnMappings {

	public static final String ROOT_DIR_NAME = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\";

	public static final String PATTERN = "*\\_meta\\*.xlsx";

	public static void exec(CosmosConnections conns) {
		log.info("Creating column aliases...");
		List<File> files = FileUtil.listFiles(new File(ROOT_DIR_NAME), PATTERN);
		for (File file : files) {
			log.info("Processing: " + file.getName());
			CreateColumnMappingsAction.exec(file, conns);
		}
		log.info("Done creating column aliases.");
	}

}
