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
public class UpdateDemoGroupTable {

	@Test
	public void doUpdate() {
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("Updating group table...");
			RawDataFileUploadParams params = BuildParamsWomensHealth.getParams("Demographics", "demo");
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
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_txt", "age_at_the_endof_measurement_year", "age", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_txt", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_txt", "ethnicity_standard_descr", "ethnicity", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_txt", "race_standard_descr", "race", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_txt", "transporation", "transportation", conn);
		// ac lot 2
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_csv", "age_at_the_endof_measurement_year", "age", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_csv", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_csv", "ethnicity_standard_descr", "ethnicity", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_csv", "race_standard_descr", "race", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_csv", "transporation", "transportation", conn);
		// ac lot 2 PP
		CreateColumnAlias.execute("womens_health_pp_demo", "prj_raw_womens_health_pp_demo", "womens_health_pp_ac_demo_nachc__ucsf__patient__demographic_csv", "age_at_the_endof_measurement_year", "age", conn);
		CreateColumnAlias.execute("womens_health_pp_demo", "prj_raw_womens_health_pp_demo", "womens_health_pp_ac_demo_nachc__ucsf__patient__demographic_csv", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_pp_demo", "prj_raw_womens_health_pp_demo", "womens_health_pp_ac_demo_nachc__ucsf__patient__demographic_csv", "ethnicity_standard_descr", "ethnicity", conn);
		CreateColumnAlias.execute("womens_health_pp_demo", "prj_raw_womens_health_pp_demo", "womens_health_pp_ac_demo_nachc__ucsf__patient__demographic_csv", "race_standard_descr", "race", conn);
		CreateColumnAlias.execute("womens_health_pp_demo", "prj_raw_womens_health_pp_demo", "womens_health_pp_ac_demo_nachc__ucsf__patient__demographic_csv", "transporation", "transportation", conn);
		// ochin
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ochin_demo_demographics_csv", "age_at_the_endof_measurement_year", "age", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ochin_demo_nachc_demographics_20201231_csv", "age_at_the_endof_measurement_year", "age", conn);
		CreateColumnAlias.execute("womens_health_pp_demo", "prj_raw_womens_health_pp_demo", "womens_health_pp_ochin_demo_nachc_demographics_20201231_csv", "age_at_the_endof_measurement_year", "age", conn);
		// denver v1
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_denver_demo_denver_health_demographics_csv", "health_insurance_type", "insurance", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_denver_demo_denver_health_demographics_csv", "education_level", "education", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_denver_demo_denver_health_demographics_csv", "accessto_care", "access_to_care", conn);
		// denver v2
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_denver_demo_denver__womens_health__v2_demo_2020_11_21_csv", "health_insurance_type", "insurance", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_denver_demo_denver__womens_health__v2_demo_2020_11_21_csv", "education_level", "education", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_denver_demo_denver__womens_health__v2_demo_2020_11_21_csv", "accessto_care", "access_to_care", conn);

		// health efficient
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_he_demo_health_efficient_ccdata__demographics4_1_19to3_31_20_csv", "ageat_start_dos", "age", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_he_demo_health_efficient_ccdata__demographics4_1_19to3_31_20_csv", "health_center", "health_center_id", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_he_demo_health_efficient_ccdata__demographics4_1_19to3_31_20_csv", "veteran", "veteran_status", conn);

	}

}
