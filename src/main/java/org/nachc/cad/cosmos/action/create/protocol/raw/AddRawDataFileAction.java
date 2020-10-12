package org.nachc.cad.cosmos.action.create.protocol.raw;

import java.sql.Connection;

import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateRawDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.UploadRawDataFileToDatabricksAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableColAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableFileAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.yaorma.database.Database;

public class AddRawDataFileAction {

	public static void execute(RawDataFileUploadParams params, Connection dbConn, Connection mySqlConn) {
		// mysql stuff
		CreateRawTableAction.execute(params, mySqlConn);
		CreateRawTableFileAction.execute(params, mySqlConn);
		CreateRawTableColAction.execute(params, mySqlConn);
		// databricks stuff
		UploadRawDataFileToDatabricksAction.execute(params, dbConn);
		CreateRawDataTableAction.execute(params, dbConn);
		CreateGrpDataTableAction.execute(params, dbConn, mySqlConn);
		Database.commit(mySqlConn);
	}

}
