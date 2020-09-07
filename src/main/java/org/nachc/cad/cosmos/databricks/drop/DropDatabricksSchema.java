package org.nachc.cad.cosmos.databricks.drop;

import java.sql.Connection;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;

import com.nach.core.util.databricks.database.DatabricksDbUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DropDatabricksSchema {

	public static void main(String[] args) {
		log.info("* * * * * * * * * * * * * * * * * * * * * * *");
		log.info("* * *                                   * * *");
		log.info("* * *    ABOUT TO DROP COSMOS SCHEMA    * * *");
		log.info("* * *                                   * * *");
		log.info("* * * * * * * * * * * * * * * * * * * * * * *");
		log.info("Getting connection...");
		Connection conn = DatabricksDbConnectionFactory.getConnection();
		log.info("Doing drop");
		DatabricksDbUtil.dropDatabase("cosmos", conn);
		log.info("Done.");
	}

}
