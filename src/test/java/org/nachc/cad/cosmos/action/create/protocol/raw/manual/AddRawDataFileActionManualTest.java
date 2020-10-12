package org.nachc.cad.cosmos.action.create.protocol.raw.manual;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.AddRawDataFileAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.group.DeleteRawDataGroupAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawDataDatabricksSchema;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableGroupRecordAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.action.create.protocol.raw.util.AddRawDataFileIntegrationTestUtil;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddRawDataFileActionManualTest {

	@Test
	public void shouldUploadFile() {
		log.info("Starting test...");
		log.info("Getting connections");
		Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		// do the set up (this adds data that would already exists in the system)
		RawDataFileUploadParams params = init(mySqlConn, dbConn);
		// do the upload
		AddRawDataFileAction.execute(params, dbConn, mySqlConn);
		Database.commit(mySqlConn);
		log.info("Done.");
	}

	private RawDataFileUploadParams init(Connection mySqlConn, Connection dbConn) {
		RawDataFileUploadParams params = AddRawDataFileIntegrationTestUtil.getParams();
		DeleteRawDataGroupAction.delete(params.getRawTableGroupCode(), dbConn, mySqlConn);
		CreateRawTableGroupRecordAction.execute(params, mySqlConn);
		//DatabricksDbUtil.dropDatabase(params.getGroupTableSchemaName(), dbConn);
		//DatabricksDbUtil.dropDatabase(params.getRawTableSchemaName(), dbConn);
		//CreateRawDataDatabricksSchema.execute(params, dbConn);
		return params;
	}

}
