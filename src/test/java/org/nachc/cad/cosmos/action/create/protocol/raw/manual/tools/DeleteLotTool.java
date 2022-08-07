package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.action.delete.DeleteLotAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteLotTool {

<<<<<<< HEAD
	private static final String PROJ = "hiv";
	
	private static final String ORG = "oachc";
	
	private static final String LOT = "2021-06-16_JA";
=======
	private static final String PROJ = "aim";
	
	private static final String ORG = "ww";
	
	private static final String LOT = "2022-02-01";
>>>>>>> 763c17ed3430f80d783c2960feda3873f899f18c
	
	public static void main(String[] args) {
		log.info("-----------------");
		log.info("JAVA VERSION: " + System.getProperty("java.version"));
		log.info("-----------------");
		CosmosConnections conns = new CosmosConnections();
		try {
			DeleteLotAction.deleteLot(PROJ, ORG, LOT, conns);
			conns.commit();
		} finally {
			conns.close();
		}
		log.info("Done.");
	}
	
}


