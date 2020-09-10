package org.nachc.cad.cosmos.integration.databricks.valueset;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import org.junit.Test;
import org.nachc.cad.cosmos.dvo.valueset.ValueSetGroupDvo;
import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.params.DatabricksParams;
import org.yaorma.dao.Dao;
import org.yaorma.util.time.TimeUtil;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.file.FileUtil;
import com.nach.core.util.guid.GuidFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadMultipleValuesetsFromSingleExcelIntegrationTest {

	private static final String PATH = "/FileStore/tables/integration-test/delete-me/group";
	
	@Test
	public void shouldUploadFile() {
		log.info("Starting test...");
		// set up file connectivity
		String fileUrl = DatabricksParams.getRestUrl();
		String fileToken = DatabricksAuthUtil.getToken();
		DatabricksFileUtil util = new DatabricksFileUtil(fileUrl, fileToken);
		// get a database connection
		log.info("Getting db connection");
		String dbUrl = DatabricksParams.getJdbcUrl();
		String dbToken = DatabricksAuthUtil.getToken();
		Connection conn = DatabricksDbUtil.getConnection(dbUrl, dbToken);
		// get the excel file
		File excel = getTestFile();
		// create the group record
		createValueSetGroup(conn, excel);
		// write the excel file to the server
		// write each sheet to the server
		log.info("Done.");
	}
	
	private File getTestFile() {
		File dir = FileUtil.getFile("/valueset/group/contraception");
		List<File> files = FileUtil.listFiles(dir, "*.xlsx");
		return files.get(0);
	}
	
	private ValueSetGroupDvo createValueSetGroup(Connection conn, File file) {
		String guid = GuidFactory.getGuid();
		ValueSetGroupDvo dvo = new ValueSetGroupDvo();
		dvo.setGuid(guid);
		dvo.setCreatedDate(TimeUtil.getNow());
		dvo.setDescription("DELETE_THIS_TEST_RECORD");
		dvo.setName(file.getName());
		dvo.setType("EXCEL_WORKBOOK");
		int cnt = Dao.doDatabricksInsert(dvo, conn);
		log.info("inserted " + cnt + " records");
		return dvo;
	}
	
}
