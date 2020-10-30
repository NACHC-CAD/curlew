package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.grouptable;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb.AddAllEncThumb;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.mysql.alias.CreateColumnAlias;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateEncGroupTable {

	@Test
	public void shouldDoUpdate() {
		log.info("Updating group table...");
		RawDataFileUploadParams params = AddAllEncThumb.getParams();
		log.info("Getting mySql connection");
		Connection mySqlConn = MySqlConnectionFactory.getCosmosConnection();
		log.info("Getting databricks connection");
		Connection dbConn = DatabricksDbConnectionFactory.getConnection();
		log.info("Updating columnAliases");
		updateColumnAliaises(mySqlConn);
		Database.commit(mySqlConn);
		log.info("DELETING DB FILE");
		DatabricksFileUtilResponse resp;
		resp = DatabricksFileUtilFactory.get().rmdir("/user/hive/warehouse/womens_health.db/enc");
		log.info("Got response (" + resp.isSuccess() + "): \n" + resp.getResponse());
		resp = DatabricksFileUtilFactory.get().rmdir("/user/hive/warehouse/womens_health.db/enc_dup_dates");
		log.info("Got response (" + resp.isSuccess() + "): \n" + resp.getResponse());
		resp = DatabricksFileUtilFactory.get().rmdir("/user/hive/warehouse/womens_health.db/enc_detail");
		log.info("Got response (" + resp.isSuccess() + "): \n" + resp.getResponse());
		log.info("UPDATING GROUP TABLE");
		CreateGrpDataTableAction.execute(params, dbConn, mySqlConn, true);
		log.info("Done.");
	}

	private void updateColumnAliaises(Connection conn) {
		// groupCode, tableSchema, tableName, colName, colAlias, conn
		CreateColumnAlias.execute("womens_health_enc", "prj_raw_womens_health_enc", "womens_health_denver_enc_denver_encounters_csv", "contraceptive_counseling_marker", "contraceptive_counseling", conn);
		CreateColumnAlias.execute("womens_health_enc", "prj_raw_womens_health_enc", "womens_health_ac_enc_nachc__ucsf__patient__encounters_txt", "dummy_id", "patient_id", conn);
		CreateColumnAlias.execute("womens_health_enc", "prj_raw_womens_health_enc", "womens_health_ac_enc_nachc__ucsf__patient__encounters_txt", "encounter_type", "enc_type", conn);
		CreateColumnAlias.execute("womens_health_enc", "prj_raw_womens_health_enc", "womens_health_denver_enc_denver_encounters_csv", "est_delivey_date", "est_delivery_date", conn);
		CreateColumnAlias.execute("womens_health_enc", "prj_raw_womens_health_enc", "womens_health_ochin_enc_encounters_feb_to_mar2020_csv", "patient_encounter", "encounter_id", conn);
		CreateColumnAlias.execute("womens_health_enc", "prj_raw_womens_health_enc", "womens_health_denver_enc_denver_encounters_csv", "pregnancy_intention_marker", "pregnancy_intention", conn);
		CreateColumnAlias.execute("womens_health_enc", "prj_raw_womens_health_enc", "womens_health_denver_enc_denver_encounters_csv", "sexually_active_marker", "sexually_active", conn);
	}

}
