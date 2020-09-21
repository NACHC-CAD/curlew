package org.nachc.cad.cosmos.init.millionhearts;

import java.sql.Connection;
import java.util.Date;

import org.junit.Test;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.BlockDefDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.DocumentDefDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.ProjectDvo;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.BlockDefProxy;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.PersonProxy;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.ProjectProxy;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.dao.Dao;
import org.yaorma.database.Data;
import org.yaorma.database.Database;
import org.yaorma.util.time.TimeUtil;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.guid.GuidFactory;
import com.nach.core.util.string.LoremIpsum;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateMillionHeartsProjectManualTest {

	@Test
	public void shouldCreateProject() {
		log.info("Creating project: MILLION_HEARTS");
		log.info("Getting connection");
		Connection conn = MySqlConnectionFactory.getCosmosConnection();
		if(exists(conn) == true) {
			log.info("Deleteing MILLION_HEARTS");
			delete(conn);
		} else {
			log.info("Project doesn't exists, no delete required.");
		}
		log.info("Creating records for MILLION_HEARTS");
		createProject(conn);
		createBlockDef(conn);
		createDocumentDef(conn);
		log.info("Finishing up");
		Database.commit(conn);
		Database.close(conn);
		log.info("Done.");
	}

	private boolean exists(Connection conn) {
		Data data = Database.query("select count(*) cnt from project where code = 'MILLION_HEARTS'", conn);
		int cnt = data.get(0).getInt("cnt");
		if(cnt > 0) {
			return true;
		} else {
			return false;
		}
	}
	
	private void delete(Connection conn) {
		String blockDefGuid = BlockDefProxy.getGuidForCode("MILLION_HEARTS_CSV", conn);
		String projectGuid = ProjectProxy.getGuidForCode("MILLION_HEARTS", conn);
		Database.update("delete from document where block in (select guid from block where block_def = ?)", blockDefGuid, conn);
		Database.update("delete from block where block_def = ?", blockDefGuid, conn);
		Database.update("delete from document_def where block_def = ?", blockDefGuid, conn);
		Database.update("delete from block_def where guid = ?", blockDefGuid, conn);
		Database.update("delete from project where guid = ?", projectGuid, conn);
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

	private void createDocumentDef(Connection conn) {
		DocumentDefDvo dvo = new DocumentDefDvo();
		String createdBy = PersonProxy.getGuidForUid("greshje", conn);
		Date now = TimeUtil.getNow();
		String blockDef = BlockDefProxy.getGuidForCode("MILLION_HEARTS_CSV", conn);
		dvo.setBlockDef(blockDef);
		dvo.setCreatedBy(createdBy);
		dvo.setUpdatedBy(createdBy);
		dvo.setCreatedDate(now);
		dvo.setUpdatedDate(now);
		createDocumentDef(0, "/FileStore/tables/prod/project/million-hearts/million-hearts-csv/info/protocol", "WORD", "PROTOCOL", "PROTOCOL", "Protocol", "Protocol for the collection of data for this data block", dvo, conn);
		createDocumentDef(1, "/FileStore/tables/prod/project/million-hearts/million-hearts-csv/csv/demo", "CSV", "DATA", "DEMO", "Demographics", "CSV File for Demographics data", dvo, conn);
		createDocumentDef(2, "/FileStore/tables/prod/project/million-hearts/million-hearts-csv/csv/enc", "CSV", "DATA", "ENC", "Encounter", "CSV File for Encounter data", dvo, conn);
		createDocumentDef(3, "/FileStore/tables/prod/project/million-hearts/million-hearts-csv/csv/dx", "CSV", "DATA", "DX", "Diagnosis", "CSV File for Diagnosis data", dvo, conn);
		createDocumentDef(4, "/FileStore/tables/prod/project/million-hearts/million-hearts-csv/csv/obs", "CSV", "DATA", "OBS", "Observation", "CSV File for Observation data", dvo, conn);
		createDocumentDef(5, "/FileStore/tables/prod/project/million-hearts/million-hearts-csv/csv/proc", "CSV", "DATA", "PROC", "Procedure", "CSV File for Procedure data", dvo, conn);
		createDocumentDef(6, "/FileStore/tables/prod/project/million-hearts/million-hearts-csv/csv/rx", "CSV", "DATA", "RX", "Prescription", "CSV File for Prescription data", dvo, conn);
	}

	private void createDocumentDef(int rowId, String dbPath, String fileType, String docRole, String dataGroup, String name, String desc, DocumentDefDvo dvo, Connection conn) {
		dvo.setGuid(GuidFactory.getGuid());
		dvo.setRowId(rowId);
		dvo.setDatabricksDir(dbPath);
		dvo.setFileType(fileType);
		dvo.setDocumentRole(docRole);
		dvo.setDataGroup(dataGroup);
		dvo.setName(name);
		dvo.setDescription(desc);
		log.info("Creating document def: " + dataGroup);
		Dao.insert(dvo, conn);
	}

}
