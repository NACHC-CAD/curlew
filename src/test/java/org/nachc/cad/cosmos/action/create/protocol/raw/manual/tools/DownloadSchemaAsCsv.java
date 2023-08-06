package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import java.io.File;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.database.Data;
import org.yaorma.database.Row;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DownloadSchemaAsCsv {

	private static final String SCHEMA_NAME = "covid_bronze_new_export";
	
	private static final String DIR = "C:\\temp\\covid\\covid-2023-04-24";
	
	public static void main(String[] args) {
		CosmosConnections conns = CosmosConnections.getConnections();
		try {
			exec(SCHEMA_NAME, new File(DIR), conns);
		} finally {
			conns.close();
		}
		log.info("Done");
	}
	
	public static void exec(String schemaName, File dir, CosmosConnections conns) {
		log.info("Making dir...");
		FileUtil.mkdirs(dir);
		log.info("Getting tables...");
		Data data = DatabricksDbUtil.showTables(schemaName, conns.getDbConnection());
		log.info("Getting data for the following tables: ");
		// echo the tables were getting data for
		for(Row row : data) {
			String tableName = row.get("tablename"); 
			tableName = schemaName + "." + tableName;
			log.info("\t" + tableName);
		}
		// get the data
		int cnt = 0;
		for(Row row : data) {
			cnt++;
			log.info("Getting data for table " + cnt + " of " + row.size());
			String tableName = row.get("tablename"); 
			log.info("\t" + tableName);
			tableName = schemaName + "." + tableName;
			log.info("Getting data for: " + tableName);
			File outFile = new File(dir, tableName + ".csv");
			DownloadTableAsCsvTool.exec(tableName, outFile, conns);
		}
	}
	
}
