package org.nachc.cad.cosmos.mysql.update.init.impl;

import java.sql.Connection;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.PersonDvo;
import org.yaorma.dao.Dao;
import org.yaorma.util.time.TimeUtil;

import com.nach.core.util.guid.GuidFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InitUsers {

	public static void init(Connection conn) {
		String guid = createAdmin(conn);
		createGresh(conn, guid);
	}

	private static String createAdmin(Connection conn) {
		log.info("Creating admin");
		PersonDvo dvo = new PersonDvo();
		// set the guid
		String guid = GuidFactory.getGuid();
		dvo.setGuid(guid);
		// set other fields
		dvo.setFname("Admin");
		dvo.setLname("User");
		dvo.setDisplayName("Administrator");
		// audit fields
		dvo.setCreatedBy(guid);
		dvo.setUpdatedBy(guid);
		dvo.setUsername("admin");
		dvo.setCreatedDate(TimeUtil.getNow());
		dvo.setUpdatedDate(TimeUtil.getNow());
		Dao.insert(dvo, conn);
		return guid;
	}
	
	private static void createGresh(Connection conn, String adminGuid) {
		log.info("Creating greshje");
		PersonDvo dvo = new PersonDvo();
		// set the guid
		String guid = GuidFactory.getGuid();
		dvo.setGuid(guid);
		// set other fields
		dvo.setFname("John");
		dvo.setLname("Gresh");
		dvo.setDisplayName("John E. Gresh");
		// audit fields
		dvo.setCreatedBy(adminGuid);
		dvo.setUpdatedBy(adminGuid);
		dvo.setUsername("greshje");
		dvo.setCreatedDate(TimeUtil.getNow());
		dvo.setUpdatedDate(TimeUtil.getNow());
		Dao.insert(dvo, conn);
	}
	
}
