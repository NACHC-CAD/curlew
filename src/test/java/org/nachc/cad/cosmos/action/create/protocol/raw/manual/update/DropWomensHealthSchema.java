package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DropWomensHealthSchema {

	@Test
	public void shouldDropSchema() {
		log.info("Starting test...");
		log.info("Getting connection");
		Connection conn = DatabricksDbConnectionFactory.getConnection();
		log.info("Dropping schema");
		DatabricksDbUtil.dropDatabase("womens_health", conn);
		log.info("Done.");
	}

}
