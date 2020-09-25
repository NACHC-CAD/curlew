package org.nachc.cad.cosmos.init.valuesets;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import org.nachc.cad.cosmos.proxy.cosmos.DocumentProxy;
import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.params.DatabricksParams;
import org.nachc.cad.cosmos.util.parser.vsac.VsacValueSetExcelParser;
import org.yaorma.dao.Dao;
import org.yaorma.util.time.TimeUtil;

import com.nach.core.util.databricks.database.DatabricksDbUtil;
import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.file.FileUtil;
import com.nach.core.util.guid.GuidFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadValueSets {

	public static void main(String[] args) {
		log.info("Starting upload...");
		log.info("Getting files");
		File dir = new File(DatabricksParams.getValueSetExcelDir());
		List<File> files = FileUtil.listFiles(dir, "*.xlsx");
		log.info("Getting connection");
		String url = DatabricksParams.getJdbcUrl();
		String token = DatabricksAuthUtil.getToken();
		Connection conn = DatabricksDbUtil.getConnection(url, token);
		int cnt = 0;
		for (File file : files) { 
			cnt++;
			log.info("---------------------------------------------------------");
			log.info("Processing file " + cnt + " of " + files.size() + ": \t" + FileUtil.getCanonicalPath(file));
			DocumentProxy excelDocDvo = uploadExcelFile(file, conn);
			DocumentProxy csvDocDvo = uploadCsvFile(file, excelDocDvo, conn);
		}
		log.info("Done.");
	}

	private static DocumentProxy uploadExcelFile(File file, Connection conn) {
		String excelPath = DatabricksParams.getValueSetExcelPath();
		// UPLOAD EXCEL FILE
		String url = DatabricksParams.getRestUrl();
		String token = DatabricksAuthUtil.getToken();
		DatabricksFileUtil fileUtil = new DatabricksFileUtil(url, token);
		fileUtil.replace(excelPath, file);
		// INSERT DOC RECORD FOR EXCEL FILE
		DocumentProxy excelDocDvo = new DocumentProxy();
		excelDocDvo.setGuid(GuidFactory.getGuid());
		excelDocDvo.setCreatedBy("greshje");
		excelDocDvo.setCreatedDate(TimeUtil.now());
		excelDocDvo.setDocumentUseType("VALUE_SET_EXCEL");
		excelDocDvo.setDocumentType("EXCEL");
		excelDocDvo.setDatabricksPath(excelPath);
		excelDocDvo.setFile(file);
		log.info("Doing DOCUMENT insert for excel file");
		Dao.doDatabricksInsert(excelDocDvo, conn);
		log.info("Done with insert");
		return excelDocDvo;
	}

	private static DocumentProxy uploadCsvFile(File excelFile, DocumentProxy excelDocDvo, Connection conn) {
		String csvPath = DatabricksParams.getValueSetCsvPath();
		String fileName = excelFile.getName() + ".csv";
		VsacValueSetExcelParser parser = new VsacValueSetExcelParser();
		log.info("Parsing file");
		parser.parse(excelFile);
		log.info("Done parsing file");
		String csv = parser.getCsvString();
		// UPLOAD CSV FILE
		String url = DatabricksParams.getRestUrl();
		String token = DatabricksAuthUtil.getToken();
		DatabricksFileUtil fileUtil = new DatabricksFileUtil(url, token);
		fileUtil.put(csvPath, csv, fileName, true);		
		// INSERT DOC RECORD FOR CSV FILE
		DocumentProxy dvo = new DocumentProxy();
		dvo.setGuid(GuidFactory.getGuid());
		dvo.setCreatedBy("greshje");
		dvo.setCreatedDate(TimeUtil.now());
		dvo.setDocumentUseType("VALUE_SET_CSV");
		dvo.setDocumentType("CSV");
		dvo.setDatabricksPath(csvPath);
		dvo.setDocumentName(fileName);
		dvo.setDocumentSize(new Long(csv.length() * 2));
		dvo.setDocumentSizeUnit("B");
		dvo.setParentGuid(excelDocDvo.getGuid());
		dvo.setParentRelType("SOURCE");
		log.info("Doing DOCUMENT insert for excel file");
		Dao.doDatabricksInsert(dvo, conn);
		log.info("Done with insert");
		return dvo;
	}
}
