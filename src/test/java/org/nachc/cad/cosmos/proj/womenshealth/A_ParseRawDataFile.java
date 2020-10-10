package org.nachc.cad.cosmos.proj.womenshealth;

import java.io.File;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.nach.core.util.excel.ExcelUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class A_ParseRawDataFile {

	public static void main(String[] args) throws Exception {
		log.info("Starting main...");
		String inDir = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\extracted\\DenverHealth\\etc\\src";
		String outDirName = inDir + "\\csv";
		String fileName = "No PHI - Denver Health - Data Pull Template - UCSF Project_DATA_ONLY.xlsx";
		File file = new File(inDir, fileName);
		File outDir = new File(outDirName);
		FileUtil.clearContents(outDir);
		log.info("Creating workbook");
		log.info("Got file: " + FileUtil.getCanonicalPath(file));
		log.info("Creating workbook");
		Workbook book = ExcelUtil.getWorkbook(file);
		for(Sheet sheet : ExcelUtil.getSheets(book)) {
			processSheet(file, sheet, outDir);
		}
		log.info("Done.");
	}

	private static void processSheet(File file, Sheet sheet, File outDir) {
		String fileName = FileUtil.getPrefix(file);
		fileName = fileName + sheet.getSheetName() + ".csv";
		File outFile = new File(outDir, fileName);
		log.info("Writing file: " + fileName);
		ExcelUtil.writeCsv(outFile, sheet);
	}
	
}
