package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.derived.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.BuildParamsWomensHealth;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.mysql.alias.CreateColumnAlias;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateDiagGroupTable {

	// TODO: JEG THESE ARE NOT TESTS (CALLED BY BUILD)
	
	private static final String DB_DIR = "/user/hive/warehouse/womens_health.db/dx";

	private static final RawDataFileUploadParams PARAMS = BuildParamsWomensHealth.getParams("Diagnosis", "dx");

	@Test
	public void doUpdate() {
		CosmosConnections conns = CosmosConnections.getConnections();
		try {
			log.info("Updating group table...");
			// mysql stuff
			log.info("Updating columnAliases");
			updateColumnAliaises(conns.getMySqlConnection());
			Database.commit(conns.getMySqlConnection());
			// databricks stuff
			log.info("DELETING DB FILE");
			DatabricksFileUtilResponse resp = DatabricksFileUtilFactory.get().rmdir(DB_DIR);
			log.info("Got response (" + resp.isSuccess() + "): \n" + resp.getResponse());
			log.info("UPDATING GROUP TABLE");
			CreateGrpDataTableAction.execute(PARAMS.getRawTableGroupCode(), conns, true);
			log.info("Done.");
		} finally {
			conns.close();
		}
	}

	public static void updateColumnAliaises(Connection conn) {
		// groupCode, tableSchema, tableName, colName, colAlias, conn
		CreateColumnAlias.execute("womens_health_dx", "prj_raw_womens_health_dx", "womens_health_ac_dx_nachc__ucsf__patient__diagnosis_txt", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_dx", "prj_raw_womens_health_dx", "womens_health_ac_dx_nachc__ucsf__patient__diagnosis_csv", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_pp_dx", "prj_raw_womens_health_pp_dx", "womens_health_pp_ac_dx_nachc__ucsf__patient__diagnosis_csv", "dummy_id", "patient_id", conn);
	}

}
