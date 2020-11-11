package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.grouptable;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.action.create.protocol.raw.databricks.CreateGrpDataTableAction;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb.AddAllRxThumb;
import org.nachc.cad.cosmos.action.create.protocol.raw.params.RawDataFileUploadParams;
import org.nachc.cad.cosmos.mysql.alias.CreateColumnAlias;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateRxGroupTable {

	@Test
	public void doUpdate() {
		log.info("Updating group table...");
		RawDataFileUploadParams params = AddAllRxThumb.getParams();
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
	}

}
