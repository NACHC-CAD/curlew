package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project;

import java.sql.Connection;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjCodeDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjectDvo;
import org.nachc.cad.cosmos.util.dvo.CosmosDvoUtil;
import org.yaorma.dao.Dao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateProjectMillionHearts {

	private static final String CODE = "million_hearts";
	
	private static final String NAME = "million_hearts";
	
	private static final String DESC = "Million Hearts";
	
	public static void createProject(Connection mySqlConn) {
		// create proj_code record
		createProjCodeRecord(mySqlConn);
		// create project record
		ProjectDvo dvo = new ProjectDvo();
		CosmosDvoUtil.init(dvo, "greshje", mySqlConn);
		dvo.setCode(CODE);
		dvo.setDescription(DESC);
		dvo.setName(NAME);
		log.info("Doing insert");
		Dao.insert(dvo, mySqlConn);
		log.info("Adding url records");
		// create the urls
		createUrls(mySqlConn, dvo);
	}

	private static void createProjCodeRecord(Connection mySqlConn) {
		ProjCodeDvo dvo = new ProjCodeDvo();
		dvo.setCode(CODE);
		dvo.setName(NAME);
		Dao.insert(dvo, mySqlConn);
	}

	private static void createUrls(Connection conn, ProjectDvo proj) {
	}

}
