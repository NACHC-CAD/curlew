package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.BuildParamsWomensHealth;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.mysql.alias.CreateColumnAlias;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdatePregGroupTable {

	@Test
	public void doUpdate() {
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("Updating group table...");
			RawDataFileUploadParams params = BuildParamsWomensHealth.getParams("Pregnancy", "preg");
			log.info("Updating columnAliases");
			updateColumnAliaises(conns.getMySqlConnection());
			Database.commit(conns.getMySqlConnection());
			log.info("UPDATING GROUP TABLE");
			CreateGrpDataTableAction.execute(params.getRawTableGroupCode(), conns, true);
		} finally {
			conns.close();
		}
		log.info("Done.");
	}

	public static void updateColumnAliaises(Connection conn) {
		// groupCode, tableSchema, tableName, colName, colAlias, conn
		CreateColumnAlias.execute("womens_health_preg", "prj_raw_womens_health_preg", "womens_health_ochin_preg_nachc_preg_supplement_20201231_csv", "pat_id", "patient_id", conn);
	}

}
