package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.grouptable;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb.AddAllDemoThumb;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.mysql.alias.CreateColumnAlias;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateDemoGroupTable {

	@Test
	public void doUpdate() {
//		log.info("Doing delete");
//		DatabricksFileUtilFactory.get().rmdir("/user/hive/warehouse/womens_health.db/demo_src");
		log.info("Updating group table...");
		RawDataFileUploadParams params = AddAllDemoThumb.getParams();
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
		// groupCode, tableSchema, tableName, colName, colAlias, conn
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_txt", "age_at_the_endof_measurement_year", "age", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_txt", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_denver_demo_denver_health_demographics_csv", "education_level", "education", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_txt", "ethnicity_standard_descr", "ethnicity", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_denver_demo_denver_health_demographics_csv", "health_insurance_type", "insurance", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_txt", "race_standard_descr", "race", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ac_demo_nachc__ucsf__patient__demographic_txt", "transporation", "transportation", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_denver_demo_denver_health_demographics_csv", "accessto_care", "access_to_care", conn);
		CreateColumnAlias.execute("womens_health_demo", "prj_raw_womens_health_demo", "womens_health_ochin_demo_demographics_csv", "age_at_the_endof_measurement_year", "age", conn);
	}
	
}
