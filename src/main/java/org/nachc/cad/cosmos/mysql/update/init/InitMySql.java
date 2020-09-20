package org.nachc.cad.cosmos.mysql.update.init;

import java.sql.Connection;

import org.nachc.cad.cosmos.mysql.update.init.impl.InitCodeTables;
import org.nachc.cad.cosmos.mysql.update.init.impl.InitUsers;

public class InitMySql {

	public static void init(Connection conn) {
		InitUsers.init(conn);
		InitCodeTables.init(conn);
	}

}
