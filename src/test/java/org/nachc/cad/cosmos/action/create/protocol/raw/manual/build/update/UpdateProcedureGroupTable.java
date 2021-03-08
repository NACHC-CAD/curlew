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
public class UpdateProcedureGroupTable {

	@Test
	public void doUpdate() {
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("Updating group table...");
			RawDataFileUploadParams params = BuildParamsWomensHealth.getParams("Procedure", "proc");
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
		// ac
		CreateColumnAlias.execute("womens_health_proc", "prj_raw_womens_health_proc", "womens_health_ac_proc_nachc__ucsf__patient__procedures_txt", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_proc", "prj_raw_womens_health_proc", "womens_health_ac_proc_nachc__ucsf__patient__procedures_txt", "procedure_description", "procedure_code_description", conn);
		//ac lot 2
		CreateColumnAlias.execute("womens_health_proc", "prj_raw_womens_health_proc", "womens_health_ac_proc_nachc__ucsf__patient__procedures_csv", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_proc", "prj_raw_womens_health_proc", "womens_health_ac_proc_nachc__ucsf__patient__procedures_csv", "procedure_description", "procedure_code_description", conn);
		//ac lot 2 PP
		CreateColumnAlias.execute("womens_health_pp_proc", "prj_raw_womens_health_pp_proc", "womens_health_pp_ac_proc_nachc__ucsf__patient__procedures_csv", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_pp_proc", "prj_raw_womens_health_pp_proc", "womens_health_pp_ac_proc_nachc__ucsf__patient__procedures_csv", "procedure_description", "procedure_code_description", conn);
	}

}
