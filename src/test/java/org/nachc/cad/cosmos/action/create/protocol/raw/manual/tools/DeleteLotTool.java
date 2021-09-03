package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import org.nachc.cad.cosmos.action.delete.DeleteLotAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

public class DeleteLotTool {

	private static final String PROJ = "integration_test";
	
	private static final String ORG = "acme";
	
	private static final String LOT = "LOT 1";
	
	public static void main(String[] args) {
		CosmosConnections conns = new CosmosConnections();
		try {
			DeleteLotAction.deleteLot(PROJ, ORG, LOT, conns);
		} finally {
			conns.close();
		}
	}
	
}
