package org.nachc.cad.cosmos.action.create.protocol.raw.manual.update;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update20210128UpdateColumnNames {

	public static void main(String[] args) {
		log.info("Updating columns...");
		CosmosConnections conns = new CosmosConnections();
		try {
			updateColName(conns, "contraceptive_med", "med_description");
			updateColName(conns, "contraceptive_med_dose", "med_dose");
			updateColName(conns, "contraceptive_med_refills", "refils");
			updateColName(conns, "womens_health_he_rx%", "med_name", "med_description");
			conns.commit();
		} finally {
			conns.close();
		}
	}

	private static void updateColName(CosmosConnections conns, String name, String alias) {
		String sqlString = "";
		sqlString += "update raw_table_col_alias \n";
		sqlString += "set col_alias = '" + alias + "' \n";
		sqlString += "where 1=1 \n";
		sqlString += "	and raw_table_name like 'womens_health_ochin_rx%' \n";
		sqlString += "	and col_name = '" + name + "' \n";
		sqlString += "  and col_alias is null \n";
		// log.info("Sql: \n" + sqlString);
		int cnt = Database.update(sqlString, conns.getMySqlConnection());
		log.info("RECORDS UPDATED:\t" + cnt + "\t" + name);
	}

	private static void updateColName(CosmosConnections conns, String org, String name, String alias) {
		String sqlString = "";
		sqlString += "update raw_table_col_alias \n";
		sqlString += "set col_alias = '" + alias + "' \n";
		sqlString += "where 1=1 \n";
		sqlString += "  and raw_table_name like '" + org + "' \n";
		sqlString += "	and col_name = '" + name + "' \n";
		sqlString += "  and col_alias is null \n";
		// log.info("Sql: \n" + sqlString);
		int cnt = Database.update(sqlString, conns.getMySqlConnection());
		log.info("RECORDS UPDATED:\t" + cnt + "\t" + name);
	}

}
