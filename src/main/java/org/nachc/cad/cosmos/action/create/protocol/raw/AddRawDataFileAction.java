package org.nachc.cad.cosmos.action.create.protocol.raw;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateRawDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.UploadRawDataFileToDatabricksAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableColAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableFileAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

public class AddRawDataFileAction {

	public static void execute(RawDataFileUploadParams params) {
		execute(params, params.isOverwriteExistingFiles());
	}

	public static void execute(RawDataFileUploadParams params, boolean isOverwrite) {
		// mysql stuff
		CosmosConnections conns = null;
		try {
			conns = CosmosConnections.getConnections();
			CreateRawTableAction.execute(params, conns.getMySqlConnection(), isOverwrite);
			CreateRawTableFileAction.execute(params, conns.getMySqlConnection(), isOverwrite);
			CreateRawTableColAction.execute(params, conns.getMySqlConnection());
		} finally {
			CosmosConnections.close(conns);
		}
		// databricks stuff
		UploadRawDataFileToDatabricksAction.execute(params, isOverwrite);
		try {
			conns = CosmosConnections.getConnections();
			CreateRawDataTableAction.execute(params, conns, true);
		} finally {
			CosmosConnections.close(conns);
		}
		// conns.commit();
	}

}
