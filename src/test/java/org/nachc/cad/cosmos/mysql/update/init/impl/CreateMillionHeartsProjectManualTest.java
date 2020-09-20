package org.nachc.cad.cosmos.mysql.update.init.impl;

import java.sql.Connection;
import java.util.Date;

import org.junit.Test;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.BlockDefDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.DocumentDefDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjectDvo;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.BlockDefProxy;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.PersonProxy;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.ProjectProxy;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;
import org.yaorma.util.time.TimeUtil;

import com.nach.core.util.guid.GuidFactory;
import com.nach.core.util.string.LoremIpsum;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateMillionHeartsProjectManualTest {

	@Test
	public void shouldCreateProject() {
		log.info("Creating project: MILLION HEARTS");
		log.info("Getting connection");
		Connection conn = MySqlConnectionFactory.getCosmosConnection();
		log.info("Creating records");
		createProject(conn);
		createBlockDef(conn);
		log.info("Finishing up");
		Database.commit(conn);
		Database.close(conn);
		log.info("Done.");
	}
	
	private void createProject(Connection conn) {
		log.info("Getting guid");
		String createdBy = PersonProxy.getGuidForUid("greshje", conn);
		log.info("Creating data object");
		ProjectDvo dvo = new ProjectDvo();
		String guid = GuidFactory.getGuid();
		dvo.setGuid(guid);
		dvo.setCode("MILLION_HEARTS");
		dvo.setDescription("Million Hearts Project:\n" + LoremIpsum.getParagraph());
		dvo.setName("Million Hearts");
		dvo.setCreatedBy(createdBy);
		dvo.setUpdatedBy(createdBy);
		Date now = TimeUtil.getNow();
		dvo.setCreatedDate(now);
		dvo.setUpdatedDate(now);
		log.info("Doing insert");
		int cnt = Dao.insert(dvo, conn);
		log.info("Done with insert: " + cnt);
	}

	private void createBlockDef(Connection conn) {
		log.info("Getting guid");
		String createdBy = PersonProxy.getGuidForUid("greshje", conn);
		log.info("Creating data object");
		BlockDefDvo dvo = new BlockDefDvo();
		String guid = GuidFactory.getGuid();
		dvo.setGuid(guid);
		dvo.setDescription("Protocol for data from Mosaic, Unity, et al.  Data include demographics, encounters, prescriptions, procedures, diagnosies, and observations.");
		dvo.setCode("MILLION_HEARTS_CSV");
		dvo.setName("CSV Files For Million Hearts Project");
		dvo.setProject(ProjectProxy.getGuidForCode("MILLION_HEARTS", conn));
		dvo.setCreatedBy(createdBy);
		dvo.setUpdatedBy(createdBy);
		Date now = TimeUtil.getNow();
		dvo.setCreatedDate(now);
		dvo.setUpdatedDate(now);
		log.info("Doing insert");
		Dao.insert(dvo, conn);
		log.info("Done with insert");
	}

	private void createDocumentDefs(Connection conn) {
		// TODO: FINISH THIS THOUGHT (JEG)
	}
	
	private void createDocumentDef(int rowId, String name, String role, Connection conn) {
		DocumentDefDvo dvo = new DocumentDefDvo();
		dvo.setBlockDef(BlockDefProxy.getGuidForCode("MILLION_HEARTS_CSV", conn));
		dvo.setFileType("CSV");
		dvo.setDocumentRole(role);
		dvo.setRowId(rowId);
		dvo.setName(name);
		dvo.setDescription("CVS data file for ");
	}
	
}


