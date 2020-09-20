package org.nachc.cad.cosmos.proxy.mysql.cosmos;

import java.sql.Connection;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.PersonDvo;
import org.yaorma.dao.Dao;

public class PersonProxy {

	public static PersonDvo getForUid(String uid, Connection conn) {
		PersonDvo rtn = Dao.find(new PersonDvo(), "username", uid, conn);
		return rtn;
	}
	
	public static String getGuidForUid(String uid, Connection conn) {
		PersonDvo dvo = getForUid(uid, conn);
		return dvo.getGuid();
	}

}
