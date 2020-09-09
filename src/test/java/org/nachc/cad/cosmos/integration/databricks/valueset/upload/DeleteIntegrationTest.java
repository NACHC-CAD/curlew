package org.nachc.cad.cosmos.integration.databricks.valueset.upload;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteIntegrationTest {

	private boolean success = false;
	
	@Test
	public void shouldExecuteQuery() {
		log.info("Starting test...");
		log.info("Getting connection...");
		Connection conn = DatabricksDbConnectionFactory.getConnection();
		execute("delete from value_set.value_set where guid = 'foobar'", conn);
		execute("use value_set;delete from value_set where guid = 'foobar'", conn);
		execute("use value_set;delete from value_set vs where vs.guid = 'foobar'", conn);
		execute("delete from value_set vs where vs.guid = 'foobar'", conn);
		execute("delete from value_set where guid = 'foobar'", conn);
		execute("DELETE FROM `value_set`.`value_set` value_set WHERE `value_set`.`guid`='4beb2cea-44f6-45ac-8c28-670cd3926986'", conn);
		execute("DELETE FROM value_set.value_set tbl WHERE tbl.guid='4beb2cea-44f6-45ac-8c28-670cd3926986'", conn, true);
		assertTrue(this.success == true);
		log.info("Done.");
	}
	
	private void execute(String sqlString, Connection conn) {
		execute(sqlString, conn, false);
	}
	
	private void execute(String sqlString, Connection conn, boolean showException) {
		try {
			log.info("Excuting query: " + sqlString);
			Database.update(sqlString, conn);
			this.success = true;
			log.info("* * * SUCCESS * * *");
		} catch(Exception exp) {
			log.info("Not a valid syntax (not a problem)");
			if(showException == true) {
				exp.printStackTrace();
				this.success = false;
			}
		}
	}

}
