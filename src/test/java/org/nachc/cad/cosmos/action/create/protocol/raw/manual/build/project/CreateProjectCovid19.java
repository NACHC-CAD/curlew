package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project;

import java.sql.Connection;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjCodeDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjUrlDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjectDvo;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateProjectCovid19 {

	public static void createProject(Connection mySqlConn) {
		// create proj_code record
		createProjCodeRecord(mySqlConn);
		// create project record
		ProjectDvo dvo = new ProjectDvo();
		CosmosDvoUtil.init(dvo, "greshje", mySqlConn);
		dvo.setCode("covid");
		dvo.setDescription("COVID-19 Project.");
		dvo.setName("COVID-19");
		log.info("Doing insert");
		Dao.insert(dvo, mySqlConn);
		log.info("Adding url records");
		// create the urls
		createUrls(mySqlConn, dvo);
	}

	private static void createProjCodeRecord(Connection mySqlConn) {
		ProjCodeDvo dvo = new ProjCodeDvo();
		dvo.setCode("covid");
		dvo.setName("COVID-19");
		Dao.insert(dvo, mySqlConn);
	}
	
	private static void createUrls(Connection conn, ProjectDvo proj) {
	}

}
