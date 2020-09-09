package org.nachc.cad.cosmos.integration.databricks.document;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import org.junit.Test;
import org.nachc.cad.cosmos.proxy.cosmos.DocumentProxy;
import org.nachc.cad.cosmos.util.databricks.database.DatabricksDbConnectionFactory;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;
import org.yaorma.dvo.DvoUtil;
import org.yaorma.util.time.TimeUtil;

import com.nach.core.util.file.FileUtil;
import com.nach.core.util.guid.GuidFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DocumentInsertSqlIntegrationTest {

	private static final String DATABRICKS_PATH = "/FileStore/tables/integration-test/global/value-set/csv";

	@Test
	public void shouldDoInsert() {
		log.info("Starting test..");
		File file = getTestFile();
		DocumentProxy dvo = new DocumentProxy();
		String guid = GuidFactory.getGuid();
		dvo.setGuid(guid);
		dvo.setCreatedBy("greshje");
		dvo.setCreatedDate(TimeUtil.now());
		dvo.setDocumentUseType("INTEGRATION_TEST");
		dvo.setDocumentType("CSV");
		dvo.setDatabricksPath(DATABRICKS_PATH);
		dvo.setFile(file);
		String sqlString = Dao.getDatabricksLiteralInsertSqlString(dvo);
		log.info("Got sqlString: \n" + sqlString);
		log.info("Getting connection...");
		Connection conn = DatabricksDbConnectionFactory.getConnection();
		log.info("Executing update...");
		Database.update(sqlString, conn);
		log.info("Done with save");
		log.info("Doing delete for: " + dvo.getGuid());
		Dao.delete(dvo, conn);
		log.info("Done.");
	}

	private File getTestFile() {
		String path = "/valueset/csv";
		File dir = FileUtil.getFile(path);
		List<File> files = FileUtil.listFiles(dir, "*.csv");
		File rtn = files.get(0);
		return rtn;
	}

}
