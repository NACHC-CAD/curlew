package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.update;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.BuildParamsWomensHealth;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.mysql.alias.CreateColumnAlias;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateRxGroupTable {

	@Test
	public void doUpdate() {
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("Doing delete");
			DatabricksFileUtilFactory.get().rmdir("/user/hive/warehouse/womens_health.db/rx");
			log.info("Updating group table...");
			RawDataFileUploadParams params = BuildParamsWomensHealth.getParams("Rx", "rx");
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
		// ac
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ac_rx_nachc__ucsf__patient__medications_txt", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ac_rx_nachc__ucsf__patient__medications_txt", "med_start_date", "start_date", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ac_rx_nachc__ucsf__patient__medications_txt", "med_stop_date", "end_date", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ac_rx_nachc__ucsf__patient__medications_txt", "med_refills", "refills", conn);
		// ac lot 2
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ac_rx_nachc__ucsf__patient__medications_csv", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ac_rx_nachc__ucsf__patient__medications_csv", "med_start_date", "start_date", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ac_rx_nachc__ucsf__patient__medications_csv", "med_stop_date", "end_date", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ac_rx_nachc__ucsf__patient__medications_csv", "med_refills", "refills", conn);
		// ac lot 2 PP
		CreateColumnAlias.execute("womens_health_pp_rx", "prj_raw_womens_health_pp_rx", "womens_health_pp_ac_rx_nachc__ucsf__patient__medications_csv", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_pp_rx", "prj_raw_womens_health_pp_rx", "womens_health_pp_ac_rx_nachc__ucsf__patient__medications_csv", "med_start_date", "start_date", conn);
		CreateColumnAlias.execute("womens_health_pp_rx", "prj_raw_womens_health_pp_rx", "womens_health_pp_ac_rx_nachc__ucsf__patient__medications_csv", "med_stop_date", "end_date", conn);
		CreateColumnAlias.execute("womens_health_pp_rx", "prj_raw_womens_health_pp_rx", "womens_health_pp_ac_rx_nachc__ucsf__patient__medications_csv", "med_refills", "refills", conn);
		// ochin
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ochin_rx_medications_csv", "generic_name", "med_description", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_ochin_rx_nachc_medications_20201231_csv", "generic_name", "med_description", conn);
		CreateColumnAlias.execute("womens_health_pp_rx", "prj_raw_womens_health_pp_rx", "womens_health_pp_ochin_rx_nachc_medications_20201231_csv", "generic_name", "med_description", conn);
		// ochin pp
		CreateColumnAlias.execute("womens_health_pp_rx", "prj_raw_womens_health_pp_rx", "womens_health_pp_ochin_rx_nachc_medications_20201231_csv", "contraceptive_med", "med_description", conn);
		CreateColumnAlias.execute("womens_health_pp_rx", "prj_raw_womens_health_pp_rx", "womens_health_pp_ochin_rx_nachc_medications_20201231_csv", "contraceptive_med_dose", "med_dose", conn);
		CreateColumnAlias.execute("womens_health_pp_rx", "prj_raw_womens_health_pp_rx", "womens_health_pp_ochin_rx_nachc_medications_20201231_csv", "contraceptive_med_dose_unit", "med_unit", conn);
		CreateColumnAlias.execute("womens_health_pp_rx", "prj_raw_womens_health_pp_rx", "womens_health_pp_ochin_rx_nachc_medications_20201231_csv", "contraceptive_med_refills", "refills", conn);
		CreateColumnAlias.execute("womens_health_pp_rx", "prj_raw_womens_health_pp_rx", "womens_health_pp_ochin_rx_nachc_medications_20201231_csv", "med_start_date", "start_date", conn);
		CreateColumnAlias.execute("womens_health_pp_rx", "prj_raw_womens_health_pp_rx", "womens_health_pp_ochin_rx_nachc_medications_20201231_csv", "med_end_date", "end_date", conn);
		// denver v1
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_denver_rx_denver_medications_csv", "contraceptive_med", "med_description", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_denver_rx_denver_medications_csv", "contraceptive_med_dose", "med_dose", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_denver_rx_denver_medications_csv", "contraceptive_med_refills", "refills", conn);
		// denver v2
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_denver_rx_denver__womens_health__v2_med_2020_11_21_csv", "contraceptive_med", "med_description", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_denver_rx_denver__womens_health__v2_med_2020_11_21_csv", "contraceptive_med_dose", "med_dose", conn);
		CreateColumnAlias.execute("womens_health_rx", "prj_raw_womens_health_rx", "womens_health_denver_rx_denver__womens_health__v2_med_2020_11_21_csv", "contraceptive_med_refills", "refills", conn);
		
	}

}
