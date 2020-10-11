package org.nachc.cad.cosmos.action.create.protocol.raw;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.CreateProtocolRawDataParams;
import org.nachc.cad.cosmos.action.create.protocol.raw.util.CreateRawTableGroupRecordIntegrationTestHelper;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawTableGroupRecordActionIntegrationTest {

	@Test
	public void shouldCreateRecord() {
		log.info("Starting test...");
		CreateProtocolRawDataParams params = CreateRawTableGroupRecordIntegrationTestHelper.getParams();
		log.info("Getting MYSQL connection");
		Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
		log.info("Getting DATABRICKS connection");
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		log.info("Cleaning up");
		CreateRawTableGroupRecordIntegrationTestHelper.cleanUp(params, mySqlConn, dbConn);
		// mysql stuff
		log.info("Creating mysql stuff");
		CreateRawTableGroupRecordAction.execute(params, mySqlConn);
		Database.commit(mySqlConn);
		// databricks stuff
		CreateRawDataDatabricksSchema.execute(params, dbConn);
		log.info("Done.");
	}

}
