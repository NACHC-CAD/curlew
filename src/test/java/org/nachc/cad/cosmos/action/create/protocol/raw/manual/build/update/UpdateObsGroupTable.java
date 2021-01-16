package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.BuildParamsWomensHealth;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.mysql.alias.CreateColumnAlias;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateObsGroupTable {

	@Test
	public void doUpdate() {
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("Updating group table...");
			RawDataFileUploadParams params = BuildParamsWomensHealth.getParams("Observation", "obs");
			log.info("Updating columnAliases");
			updateColumnAliaises(conns.getMySqlConnection());
			Database.commit(conns.getMySqlConnection());
			log.info("UPDATING GROUP TABLE");
			CreateGrpDataTableAction.execute(params.getRawTableGroupCode(), conns, true);
			log.info("Done.");
		} finally {
			conns.close();
		}
	}

	public static void updateColumnAliaises(Connection conn) {
		// groupCode, tableSchema, tableName, colName, colAlias, conn
		CreateColumnAlias.execute("womens_health_pp_obs", "prj_raw_womens_health_pp_obs", "womens_health_pp_ac_obs_nachc__ucsf__patient__observations_2_txt", "dummy_id", "patient_id", conn);
	}

}
