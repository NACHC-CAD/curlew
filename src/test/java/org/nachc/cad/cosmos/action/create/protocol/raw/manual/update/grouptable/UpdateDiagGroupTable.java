package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.grouptable;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb.AddAllDxThumb;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.mysql.alias.CreateColumnAlias;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateDiagGroupTable {

	private static final String DB_DIR = "/user/hive/warehouse/womens_health.db/diag";

	private static final RawDataFileUploadParams PARAMS = AddAllDxThumb.getParams();
	
	@Test
	public void doUpdate() {
		log.info("Updating group table...");
		// mysql stuff
		log.info("Getting mySql connection");
		Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
		log.info("Updating columnAliases");
		updateColumnAliaises(mySqlConn);
		Database.commit(mySqlConn);
		// databricks stuff
		log.info("Getting databricks connection");
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		log.info("DELETING DB FILE");
		DatabricksFileUtilResponse resp = DatabricksFileUtilFactory.get().rmdir(DB_DIR);
		log.info("Got response (" + resp.isSuccess() + "): \n" + resp.getResponse());
		log.info("UPDATING GROUP TABLE");
		CreateGrpDataTableAction.execute(PARAMS, dbConn, mySqlConn, true);
		log.info("Done.");
	}

	private void updateColumnAliaises(Connection conn) {
		// groupCode, tableSchema, tableName, colName, colAlias, conn
		CreateColumnAlias.execute("womens_health_dx", "prj_raw_womens_health_dx", "womens_health_ac_dx_nachc__ucsf__patient__diagnosis_txt", "dummy_id", "patient_id", conn);
	}

}
