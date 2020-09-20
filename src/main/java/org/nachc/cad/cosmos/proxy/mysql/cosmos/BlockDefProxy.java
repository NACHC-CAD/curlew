package org.nachc.cad.cosmos.proxy.mysql.cosmos;

import java.sql.Connection;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.BlockDefDvo;
import org.yaorma.dao.Dao;

public class BlockDefProxy {

	public static BlockDefDvo getForCode(String code, Connection conn) {
		return Dao.find(new BlockDefDvo(), "code", code, conn);
	}
	
	public static String getGuidForCode(String code, Connection conn) {
		BlockDefDvo dvo = getForCode(code, conn);
		return dvo.getGuid();
	}
	
}
