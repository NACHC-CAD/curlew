package org.nachc.cad.cosmos.util.dao;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import org.nachc.cad.cosmos.dvo.valueset.ValueSetGroupMemberDvo;
import org.nachc.cad.cosmos.util.parser.vsac.ValueSetGroupParser;
import org.yaorma.dao.Dao;
import org.yaorma.dvo.Dvo;

import com.nach.core.util.guid.GuidFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ValueSetGroupDao {

	public static void insertValueSetGroup(File excel, Connection conn) {
		log.info("Inserting value set group");
		ValueSetGroupParser parser = new ValueSetGroupParser();
		String guid = GuidFactory.getGuid();
		parser.init(excel, guid);
		List<ValueSetGroupMemberDvo> dvoList = parser.getAsDvoList();
		log.info("Inserting " + dvoList.size() + " records");
		int cnt = 0;
		for (ValueSetGroupMemberDvo dvo : dvoList) {
			cnt++;
			log.info("  " + cnt + " of " + dvoList.size() + "\t" + dvo.getValueSetGroupGuid() + "\t" + dvo.getValueSetName());
			Dao.doDatabricksInsert(dvo, conn);
		}
		log.info("Done inserting value set group");
	}

}
