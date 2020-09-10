package org.nachc.cad.cosmos.integration.databricks.valueset;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.Test;
import org.nachc.cad.cosmos.dvo.cosmos.DocumentDvo;
import org.nachc.cad.cosmos.dvo.valueset.ValueSetGroupDvo;
import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.params.DatabricksParams;
import org.yaorma.dao.Dao;
import org.yaorma.util.time.TimeUtil;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.excel.ExcelUtil;
import com.nach.core.util.file.FileUtil;
import com.nach.core.util.guid.GuidFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadMultipleValuesetsFromSingleExcelIntegrationTest {

	private static final String PATH = "/FileStore/tables/integration-test/delete-me/group";
	
	@Test
	public void shouldUploadFile() {
		log.info("Starting test...");
		String databricksExcelPath = PATH + "/excel";
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
		// create the document record and post the document
		log.info("Writing document record to database");
		DocumentDvo excelDocDvo = createDocument(conn, excel, databricksExcelPath);
		DatabricksFileUtil fileUtil = new DatabricksFileUtil(fileUrl, fileToken);
		log.info("Writing file to databricks");
		fileUtil.replace(databricksExcelPath, excel);
		// create the group record
		log.info("Creating value set group");
		createValueSetGroup(conn, excel, excelDocDvo);
		// write each sheet to the server
		Workbook book = ExcelUtil.getWorkbook(excel);
		for(Sheet sheet : book) {
			
		}
		log.info("Done.");
	}
	
	private File getTestFile() {
		File dir = FileUtil.getFile("/valueset/group/contraception");
		List<File> files = FileUtil.listFiles(dir, "*.xlsx");
		return files.get(0);
	}
	
	private DocumentDvo createDocument(Connection conn, File excel, String databricksExcelPath) {
		DocumentDvo dvo = new DocumentDvo();
		dvo.setCreatedBy(this.getClass().getCanonicalName());
		dvo.setCreatedDate(TimeUtil.getNow());
		dvo.setDatabricksPath(databricksExcelPath);
		dvo.setDocumentName(excel.getName());
		dvo.setDocumentSize(FileUtil.getSize(excel));
		dvo.setDocumentSizeUnit("B");
		dvo.setDocumentType("EXCEL");
		dvo.setDocumentUseType("VALUE_SET_GROUP");
		dvo.setGuid(GuidFactory.getGuid());
		log.info("Doing insert for document");
		Dao.doDatabricksInsert(dvo, conn);
		log.info("Done with insert");
		return dvo;
	}

	private ValueSetGroupDvo createValueSetGroup(Connection conn, File file, DocumentDvo docDvo) {
		String guid = GuidFactory.getGuid();
		ValueSetGroupDvo dvo = new ValueSetGroupDvo();
		dvo.setGuid(guid);
		dvo.setDocumentGuid(docDvo.getGuid());
		dvo.setCreatedDate(TimeUtil.getNow());
		dvo.setDescription("DELETE_THIS_TEST_RECORD");
		dvo.setName(file.getName());
		dvo.setType("EXCEL_WORKBOOK");
		dvo.setCreatedBy(this.getClass().getCanonicalName());
		dvo.setCreatedDate(TimeUtil.getNow());
		log.info("Doing insert for value set group");
		Dao.doDatabricksInsert(dvo, conn);
		log.info("Done with insert");
		return dvo;
	}
	
}
