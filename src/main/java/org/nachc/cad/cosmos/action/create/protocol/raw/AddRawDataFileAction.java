package org.nachc.cad.cosmos.action.create.protocol.raw;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateRawDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.UploadRawDataFileToDatabricksAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableColAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableFileAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;

public class AddRawDataFileAction {

	public static void execute(RawDataFileUploadParams params, CosmosConnections conns) {
		execute(params, conns, params.isOverwriteExistingFiles());
	}

	public static void execute(RawDataFileUploadParams params, CosmosConnections conns, boolean isOverwrite) {
		// mysql stuff
		CreateRawTableAction.execute(params, conns.getMySqlConnection(), isOverwrite);
		CreateRawTableFileAction.execute(params, conns.getMySqlConnection(), isOverwrite);
		CreateRawTableColAction.execute(params, conns.getMySqlConnection());
		// databricks stuff
		UploadRawDataFileToDatabricksAction.execute(params, conns, isOverwrite);
		// conns.commit();
		// conns.resetConnections();
		CreateRawDataTableAction.execute(params, conns, true);
		// conns.commit();
	}

}
