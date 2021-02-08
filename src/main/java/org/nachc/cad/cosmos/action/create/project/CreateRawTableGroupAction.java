package org.nachc.cad.cosmos.action.create.project;

import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawDataDatabricksSchemaAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableGroupRecordAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.action.delete.DeleteRawDataGroupAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

public class CreateRawTableGroupAction {

	/**
	 * 
	 * This method creates the new table entries for a new raw table group in an existing projec.
	 * 
	 * This method will delete all existing entities for the given project from both
	 * Databricks and MySql.
	 * 
	 */

	public static void exec(RawDataFileUploadParams params, CosmosConnections conns) {
		createDatabricksEntitiesForRawTableGroup(conns, params);
		createMySqlSchemaForRawTableGroup(conns, params);
	}

	private static void createDatabricksEntitiesForRawTableGroup(CosmosConnections conns, RawDataFileUploadParams params) {
		// clear the files off of data bricks
		DatabricksFileUtilFactory.get().rmdir(params.getDatabricksFileLocation());
		// drop the raw and grp databases in data bricks
		DatabricksDbUtil.dropDatabase(params.getGroupTableSchemaName(), conns.getDbConnection(), conns);
		DatabricksDbUtil.dropDatabase(params.getRawTableSchemaName(), conns.getDbConnection(), conns);
		// create the raw and grp databases in data bricks
		CreateRawDataDatabricksSchemaAction.execute(params, conns.getDbConnection());
	}

	private static void createMySqlSchemaForRawTableGroup(CosmosConnections conns, RawDataFileUploadParams params) {
		// delete the old
		DeleteRawDataGroupAction.delete(params.getRawTableGroupCode(), conns);
		// create a new empty raw data group
		CreateRawTableGroupRecordAction.execute(params, conns.getMySqlConnection());
	}

}
