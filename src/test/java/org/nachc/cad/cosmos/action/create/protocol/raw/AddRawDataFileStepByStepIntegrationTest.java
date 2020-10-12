package org.nachc.cad.cosmos.action.create.protocol.raw;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateRawDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.UploadRawDataFileToDatabricksAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawDataDatabricksSchema;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableColAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableFileAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableGroupMembershipTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.mysql.CreateRawTableGroupRecordAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.action.create.protocol.raw.util.AddRawDataFileIntegrationTestUtil;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AddRawDataFileStepByStepIntegrationTest {

	@Test
	public void shouldCreateRecord() {
		log.info("Starting test...");
		RawDataFileUploadParams params = AddRawDataFileIntegrationTestUtil.getParams();
		log.info("Getting MYSQL connection");
		Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
		doMySqlCreate(params, mySqlConn);
		doDatabricksCreate(params, mySqlConn);
		log.info("Done.");
	}

	private void doMySqlCreate(RawDataFileUploadParams params, Connection mySqlConn) {
		log.info("Cleaning up");
		AddRawDataFileIntegrationTestUtil.cleanupMySql(params, mySqlConn);
		log.info("Creating mysql stuff");
		CreateRawTableGroupRecordAction.execute(params, mySqlConn);
		CreateRawTableAction.execute(params, mySqlConn);
		CreateRawTableFileAction.execute(params, mySqlConn);
		CreateRawTableColAction.execute(params, mySqlConn);
		CreateRawTableGroupMembershipTable.execute(params, mySqlConn);
		Database.commit(mySqlConn);
	}

	private void doDatabricksCreate(RawDataFileUploadParams params, Connection mySqlConn) {
		// get connection and clean up
		log.info("Getting DATABRICKS connection");
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		log.info("Cleaning up");
		AddRawDataFileIntegrationTestUtil.cleanUpDatabricks(params, dbConn);
		// actual create stuff
		log.info("Creating Databricks stuff");
		CreateRawDataDatabricksSchema.execute(params, dbConn);
		UploadRawDataFileToDatabricksAction.execute(params, dbConn);
		CreateRawDataTableAction.execute(params, dbConn);
		CreateGrpDataTableAction.execute(params, dbConn, mySqlConn);
	}

}
