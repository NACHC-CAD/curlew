package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.action.delete.DeleteLotAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteLotTool {

	private static final String PROJ = "ohdsi_vocab";
	
	private static final String ORG = "omop";
	
	private static final String LOT = "2023-08-02";

	public static void main(String[] args) {
		log.info("-----------------");
		log.info("JAVA VERSION: " + System.getProperty("java.version"));
		log.info("-----------------");
		CosmosConnections conns = CosmosConnections.getConnections();
		try {
			DeleteLotAction.deleteLot(PROJ, ORG, LOT, conns);
			conns.commit();
		} finally {
			conns.close();
		}
		log.info("Done.");
	}
	
}


