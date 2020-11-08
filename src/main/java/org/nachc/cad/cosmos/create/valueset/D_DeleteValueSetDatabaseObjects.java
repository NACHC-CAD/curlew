package org.nachc.cad.cosmos.create.valueset;

import java.sql.Connection;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.yaorma.database.Database;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class D_DeleteValueSetDatabaseObjects {

	public static void main(String[] args) {
		delete();
	}

	public static void delete() {
		Connection conn = null;
		try {
			log.info("Getting connection...");
			conn = DatabricksDbConnectionFactory.getConnection();
			String schemaName =  A_ParametersForValueSetSchema.SCHEMA_NAME;
			log.info("Dropping database: " + schemaName);
			DatabricksDbUtil.dropDatabase(schemaName, conn);
			log.info("Done dropping: " + schemaName);
		} finally {
			Database.close(conn);
		}
	}
	
}
