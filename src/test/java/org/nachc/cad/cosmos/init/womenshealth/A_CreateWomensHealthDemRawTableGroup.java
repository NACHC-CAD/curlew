package org.nachc.cad.cosmos.init.womenshealth;

import java.sql.Connection;

import org.junit.Test;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableGroupDvo;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class A_CreateWomensHealthDemRawTableGroup {

	@Test
	public void shouldCreateRawTableGroup() {
		log.info("Starting test...");
		log.info("Getting connection...");
		Connection conn = MySqlConnectionFactory.getCosmosConnection();
		log.info("Creating data object");
		RawTableGroupDvo dvo = new RawTableGroupDvo();
		CosmosDvoUtil.init(dvo, "greshje", conn);
		dvo.setCode("WOMENS_HEALTH_DEM");
		dvo.setName("Women's Health Demographics Table");
		dvo.setDescription("This table contains the raw data for DEMOGRAPHICS data from all sources for this project.");
		dvo.setRawTableSchema("prj_raw_womens_health");
		dvo.setGroupTableSchema("prj_grp_womens_health");
		dvo.setGroupTableName("demographics");
		log.info("Doing insert...");
		Dao.insert(dvo, conn);
		Database.commit(conn);
		Database.close(conn);
		log.info("Done.");
	}
	
}
