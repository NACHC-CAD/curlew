package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import java.io.File;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DownloadTableAsCsvTool {

	private static final String TABLE_NAME = "hiv_bronze.demo";
	
	private static final File FILE = new File("C:\\temp\\hiv_bronze.demo.csv");
	
	public static void main(String[] args) {
		log.info("Getting connection...");
		CosmosConnections conns = new CosmosConnections();
		try {
			exec(TABLE_NAME, FILE, conns);
		} finally {
			conns.close();
		}
		log.info("Done.");
	}
	
	public static void exec(String tableName, File outFile, CosmosConnections conns) {
		log.info("Doing download for: " + tableName);
		String sqlString = "select * from " + tableName;
		Database.exportResultsAsCsv(sqlString, outFile, conns.getDbConnection());
		log.info("Done with " + tableName);
	}
	
}
