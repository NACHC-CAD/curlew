package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.BuildParams;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.mysql.alias.CreateColumnAlias;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateRxGroupTable {

	@Test
	public void doUpdate() {
		log.info("Doing delete");
		DatabricksFileUtilFactory.get().rmdir("/user/hive/warehouse/womens_health.db/rx");
		log.info("Updating group table...");
		RawDataFileUploadParams params = BuildParams.getParams("Rx", "rx");
		log.info("Getting mySql connection");
		Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
		log.info("Getting databricks connection");
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		log.info("Updating columnAliases");
		updateColumnAliaises(mySqlConn);
		Database.commit(mySqlConn);
		log.info("UPDATING GROUP TABLE");
		CreateGrpDataTableAction.execute(params, dbConn, mySqlConn, true);
		log.info("Done.");
	}

	private void updateColumnAliaises(Connection conn) {
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ac_rx_nachc__ucsf__patient__medications_txt", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ac_rx_nachc__ucsf__patient__medications_txt", "med_start_date", "start_date", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ac_rx_nachc__ucsf__patient__medications_txt", "med_stop_date", "end_date", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_denver_rx_denver_medications_csv", "contraceptive_med", "med_description", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ochin_rx_medications_csv", "generic_name", "med_description", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_denver_rx_denver_medications_csv", "contraceptive_med_dose", "med_dose", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_denver_rx_denver_medications_csv", "contraceptive_med_refills", "refills", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ac_rx_nachc__ucsf__patient__medications_txt", "med_refills", "refills", conn);
	}

}
