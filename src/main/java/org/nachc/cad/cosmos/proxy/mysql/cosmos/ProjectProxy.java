package org.nachc.cad.cosmos.proxy.mysql.cosmos;

import java.sql.Connection;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjectDvo;
import org.yaorma.dao.Dao;

public class ProjectProxy {

	public static ProjectDvo getForCode(String code, Connection conn) {
		ProjectDvo rtn = Dao.find(new ProjectDvo(), "code", code, conn);
		return rtn;
	}
	
	public static String getGuidForCode(String code, Connection conn) {
		ProjectDvo dvo = getForCode(code, conn);
		String rtn = dvo.getGuid();
		return rtn;
	}
	
}
