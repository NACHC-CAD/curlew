package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.action.delete.DeleteLotAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteLotTool {

	private static final String PROJ = "hiv";
	
	private static final String ORG = "oachc";
	
	private static final String LOT = "2021-06-16_JA";
	
	public static void main(String[] args) {
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


