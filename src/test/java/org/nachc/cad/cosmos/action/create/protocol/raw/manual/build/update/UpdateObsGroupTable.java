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
public class UpdateObsGroupTable {

	@Test
	public void doUpdate() {
		CosmosConnections conns = CosmosConnections.getConnections();
		try {
			log.info("Updating group table...");
			RawDataFileUploadParams params = BuildParamsWomensHealth.getParams("Observation", "obs");
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
		CreateColumnAlias.execute("womens_health_obs", "prj_raw_womens_health_obs", "womens_health_ac_obs_nachc__ucsf__patient__observations_txt", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_obs", "prj_raw_womens_health_obs", "womens_health_ac_obs_nachc__ucsf__patient__observations_txt", "fglu_resut", "fglu_result", conn);
		CreateColumnAlias.execute("womens_health_obs", "prj_raw_womens_health_obs", "womens_health_ac_obs_nachc__ucsf__patient__observations_txt", "hba1_cresult", "hba1_c_result", conn);
		CreateColumnAlias.execute("womens_health_obs", "prj_raw_womens_health_obs", "womens_health_ac_obs_nachc__ucsf__patient__observations_2_txt", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_obs", "prj_raw_womens_health_obs", "womens_health_ac_obs_nachc__ucsf__patient__observations_2_txt", "fglu_resut", "fglu_result", conn);
		CreateColumnAlias.execute("womens_health_obs", "prj_raw_womens_health_obs", "womens_health_ac_obs_nachc__ucsf__patient__observations_2_txt", "hba1_cresult", "hba1_c_result", conn);
	}

}
