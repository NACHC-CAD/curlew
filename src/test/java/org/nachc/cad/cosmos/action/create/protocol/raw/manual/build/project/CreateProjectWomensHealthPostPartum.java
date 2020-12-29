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
public class CreateProjectWomensHealthPostPartum {

	public static void createProject(Connection mySqlConn) {
		// create proj_code record
		createProjCodeRecord(mySqlConn);
		// create project record
		ProjectDvo dvo = new ProjectDvo();
		CosmosDvoUtil.init(dvo, "greshje", mySqlConn);
		dvo.setCode("womens_health_pp");
		dvo.setDescription("Informatics support for the goals of the Women''s Health projects including using existing value sets and electronic clinical quality measures, develop and enhance current clinical decision support tools that can be used at the point of care to increase adoption of clinical contraception guidelines.");
		dvo.setName("Women's Health Post Partum");
		log.info("Doing insert");
		Dao.insert(dvo, mySqlConn);
		log.info("Adding url records");
		// create the urls
		createUrls(mySqlConn, dvo);
	}

	private static void createProjCodeRecord(Connection mySqlConn) {
		ProjCodeDvo dvo = new ProjCodeDvo();
		dvo.setCode("womens_health_pp");
		dvo.setName("Womens Health Post Partum");
		Dao.insert(dvo, mySqlConn);
	}
	
	private static void createUrls(Connection conn, ProjectDvo proj) {
		ProjUrlDvo dvo;
		// contraception url
		dvo = new ProjUrlDvo();
		CosmosDvoUtil.init(dvo, "greshje", conn);
		dvo.setSortOrder(1);
		dvo.setProject("womens_health_pp");
		dvo.setLinkText("Women's Health Contraception Confluence Page");
		dvo.setUrl("https://confluence.nachc.org/pages/viewpage.action?pageId=6521529");
		dvo.setUrlType("confluence_url");
		dvo.setUrlDescription("NACHC Homepage for the Women's Post Partum");
		log.info("Adding contraception url");
		Dao.insert(dvo, conn);
		// post partum
		dvo = new ProjUrlDvo();
		CosmosDvoUtil.init(dvo, "greshje", conn);
		dvo.setSortOrder(2);
		dvo.setProject("womens_health_pp");
		dvo.setLinkText("Women's Health Post Partum Confluence Page");
		dvo.setUrl("https://confluence.nachc.org/pages/viewpage.action?pageId=6521299");
		dvo.setUrlType("confluence_url");
		dvo.setUrlDescription("NACHC Homepage for the Women's Health Post Partum Program");
		log.info("Adding post partum url");
		Dao.insert(dvo, conn);
	}

}
