package org.nachc.cad.cosmos.action.create.protocol.raw;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.after.CreateRawTableGroupRecordIntegrationTestHelper;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.CreateProtocolRawDataParams;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateRawTableGroupRecordActionIntegrationTest {

	@Test
	public void shouldCreateRecord() {
		log.info("Starting test...");
		CreateProtocolRawDataParams params = CreateRawTableGroupRecordIntegrationTestHelper.getParams();
		log.info("Getting connection");
		Connection conn = MySqlConnectionFactory.getCosmosConnection();
		log.info("Cleaning up");
		CreateRawTableGroupRecordIntegrationTestHelper.cleanUp(params, conn);
		log.info("Creating record");
		CreateRawTableGroupRecordAction.execute(params, conn);
		Database.commit(conn);
		log.info("Done.");
	}
	
}
