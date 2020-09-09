package org.nachc.cad.cosmos.integration.databricks.valueset.upload;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.dvo.valueset.ValueSetDvo;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.yaorma.dao.Dao;

import com.nach.core.util.guid.GuidFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InsertValueSetRowIntegrationTest {

	@Test
	public void shouldInsertRecord() {
		try {
			log.info("Starting test...");
			// create the dvo
			ValueSetDvo dvo = new ValueSetDvo();
			dvo.setGuid(GuidFactory.getGuid());
			// do the insert
			String sqlString = Dao.createInsertSqlString(dvo);
			log.info("Got insert string: \n" + sqlString);
			log.info("Getting connection...");
			Connection conn = DatabricksDbConnectionFactory.getConnection();
			log.info("Doing insert");
			Dao.insert(dvo, conn);
			log.info("Doing find");
			ValueSetDvo found = Dao.find(new ValueSetDvo(), "guid", dvo.getGuid(), conn);
			log.info("Created dvo: " + dvo.getGuid());
			log.info("Found dvo:   " + found.getGuid());
			assertTrue(dvo.getGuid().equals(found.getGuid()));
			log.info("Doing delete");
			String deleteString = Dao.getDeleteString(dvo);
			log.info("Delete String: \n" + deleteString);
			Dao.delete(dvo, conn);
			log.info("Done.");
		} catch(Exception exp) {
			exp.printStackTrace();
			throw new RuntimeException(exp);
		}
	}
	
}
