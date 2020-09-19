package org.nachc.cad.cosmos.mysql.update.init.impl;

import java.sql.Connection;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.PersonDvo;
import org.yaorma.util.time.TimeUtil;

import com.nach.core.util.guid.GuidFactory;

public class InitUsers {

	public static void init(Connection conn) {
		String guid = GuidFactory.getGuid();
		PersonDvo dvo = new PersonDvo();
		dvo.setGuid(guid);
		dvo.setCreatedBy("greshje");
		dvo.setCreatedDate();
	}
	
}
