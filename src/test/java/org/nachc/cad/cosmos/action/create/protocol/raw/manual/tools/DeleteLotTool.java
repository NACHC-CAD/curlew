package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.action.delete.DeleteLotAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteLotTool {

	private static final String PROJ = "nachc_term";
	
	private static final String ORG = "nachc";
	
	private static final String LOT = "2022-04-28-eth";

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


