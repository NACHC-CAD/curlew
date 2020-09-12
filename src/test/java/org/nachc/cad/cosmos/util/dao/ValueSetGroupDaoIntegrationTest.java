package org.nachc.cad.cosmos.util.dao;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import org.junit.Test;
import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.params.DatabricksParams;
import org.nachc.cad.cosmos.util.parser.vsac.ValueSetGroupParser;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.file.FileUtil;
import com.nach.core.util.guid.GuidFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ValueSetGroupDaoIntegrationTest {

	@Test
	public void shouldInsertRecords() {
		log.info("Starting test...");
		log.info("Getting file");
		File file = getTestFile();
		log.info("Getting connection");
		String url = DatabricksParams.getJdbcUrl();
		String token = DatabricksAuthUtil.getToken();
		Connection conn = DatabricksDbUtil.getConnection(url, token);
		log.info("Doing parse");
		ValueSetGroupParser parser = new ValueSetGroupParser();
		String guid = GuidFactory.getGuid();
		parser.init(file, guid);
		log.info("Doing insert");
		ValueSetGroupDao.insertValueSetGroup(file, conn);
		log.info("Done");
	}
	
	private File getTestFile() {
		File dir = FileUtil.getFile("/valueset/group/contraception");
		List<File> files = FileUtil.listFiles(dir, "*.xlsx");
		return files.get(0);
	}
	
}
