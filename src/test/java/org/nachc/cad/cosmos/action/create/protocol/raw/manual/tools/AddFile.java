package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import java.io.File;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;

import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddFile {

	private static final String PATH = "/FileStore/meta/host";
	
	private static final File FILE = new File("C:\\this_is_dev");
	
	public static void main(String[] args) {
		log.info("Putting file on Databricks: " + PATH + "/" + FILE.getName());
		DatabricksFileUtilResponse resp = DatabricksFileUtilFactory.get().put(PATH, FILE, false);
		log.info("SUCCESS: " + resp.isSuccess());
		log.info("Got response: \n" + resp.getResponse());
		log.info("Done.");
	}
	
}
