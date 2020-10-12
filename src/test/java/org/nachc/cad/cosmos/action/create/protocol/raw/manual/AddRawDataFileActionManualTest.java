package org.nachc.cad.cosmos.action.create.protocol.raw.manual;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.AddRawDataFileAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawDataDatabricksSchema;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableGroupRecordAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.action.create.protocol.raw.util.AddRawDataFileIntegrationTestUtil;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;

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
	}

	private RawDataFileUploadParams init(Connection mySqlConn, Connection dbConn) {
		RawDataFileUploadParams params = AddRawDataFileIntegrationTestUtil.getParams();
		AddRawDataFileIntegrationTestUtil.cleanUp(params, mySqlConn, dbConn);
		CreateRawTableGroupRecordAction.execute(params, mySqlConn);
		CreateRawDataDatabricksSchema.execute(params, dbConn);
		return params;
	}

}
