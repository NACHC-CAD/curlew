package org.nachc.cad.cosmos.action.delete;

import org.junit.Test;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.project.UploadDir;
import org.yaorma.database.Data;
import org.yaorma.database.Database;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteLotActionIntegrationTest {

	private static final String LOT1_DIR = FileUtil.getCanonicalPath("/upload/acme/1942-08-01-TEST_ACME-Lot1");

	private static final String LOT2_DIR = FileUtil.getCanonicalPath("/upload/acme/1942-08-01-TEST_ACME-Lot2");

	private static final String LOT3_DIR = FileUtil.getCanonicalPath("/upload/acme/1942-08-01-TEST_ACME-Lot3");

	private static final String LOT1 = "LOT 1";

	private static final String LOT2 = "LOT 2";

	private static final String LOT3 = "LOT 3";

	private static final String PROJECT = "integration_test";

	private static final String ORG = "acme";

	@Test
	public void shouldDeleteLot() {
		log.info("Starting test...");
		CosmosConnections conns = new CosmosConnections();
		try {
			// add the test data lots
			addLot(conns, LOT1, LOT1_DIR);
			addLot(conns, LOT2, LOT2_DIR);
			addLot(conns, LOT3, LOT3_DIR);
			// delete lot 2
			log.info("Deleting lot 2");
			DeleteLotAction.deleteLot(PROJECT, ORG, LOT2, conns);
			conns.commit();
			log.info("Done deleting lot 2");
		} finally {
			conns.close();
		}
		log.info("Done.");
	}

	private void addLot(CosmosConnections conns, String lot, String fileLocation) {
		log.info("Uploading lot: " + lot);
		if (lotExists(lot, conns) == false) {
			UploadDir.exec(fileLocation, "greshje", conns);
			conns.commit();
		}
		log.info("Done uploading lot 1");
	}

	private boolean lotExists(String lot, CosmosConnections conns) {
		String sqlString = "select * from raw_table_col_detail where project = ? and org_code = ? and data_lot = ?";
		String[] params = { PROJECT, ORG, lot };
		Data data = Database.query(sqlString, params, conns.getMySqlConnection());
		log.info("Got " + data.size() + " records.");
		if (data.size() > 0) {
			log.info("Lot exists");
			return true;
		} else {
			log.info("Lot does not exist");
			return false;
		}
	}

}
