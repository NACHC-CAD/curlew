package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import java.sql.Connection;

import org.nachc.cad.cosmos.util.guid.GuidUtil;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.nachc.cad.cosmos.util.mysql.params.MySqlParams;
import org.yaorma.database.Database;

import com.nach.core.util.web.security.PasswordUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChangePassword {

	private static final String uid = "admin";
	
	private static final String pwd = "BobsBurgers";
	
	public static void main(String[] args) {
		Connection conn = MySqlConnectionFactory.getCosmosConnection();
		try {
			log.info("Updateing password for: " + uid);
			String salt = GuidUtil.getGuid();
			String key = MySqlParams.getKey();
			String password = new PasswordUtil(salt, key).create(pwd);
			log.info("Salt: " + salt);
			log.info("Key:  " + key);
			String sqlString = "update person set password = ?, salt = ? where username = ? ";
			String[] params = {password, salt, uid};
			int cnt = Database.update(sqlString, params, conn);
			log.info("Updated password (" + cnt + " rows updated)");
			Database.commit(conn);
			log.info("Done.");
		} finally {
			Database.close(conn);
		}
	}
	
}
