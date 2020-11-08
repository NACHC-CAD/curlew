package org.nachc.cad.cosmos.create.valueset;

import java.io.File;
import java.util.List;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.nachc.cad.cosmos.util.vsac.VsacValueSetParser;

import com.nach.core.util.excel.ExcelUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class E_ParseValueSetFiles {

	public static void main(String[] args) {
		parse();
	}

	public static void parse() {
		log.info("Starting parse...");
		// get the locations
		File excelDir = new File(A_ParametersForValueSetSchema.EXCEL_FILE_ROOT);
		File csvDir = new File(A_ParametersForValueSetSchema.CSV_FILE_ROOT);
		File metaDir = new File(A_ParametersForValueSetSchema.META_FILE_ROOT);
		// get the excel files
		List<File> files = FileUtil.listFiles(excelDir, "*.xlsx");
		// create the metadata excel objects
		Workbook book = ExcelUtil.createNewWorkbook();
		Sheet metaData = book.createSheet();
		// iterate through each file
		log.info("Parsing " + files.size() + " files...");
		int cnt = 0;
		for (File file : files) {
			cnt++;
			log.info("\tFile " + cnt + " of " + files.size() + ": " + file.getName());
			VsacValueSetParser.parseFile(file, csvDir, metaData);
		}
		log.info("Writing meta data");
		VsacValueSetParser.writeMetaData(metaDir, metaData);
		log.info("Done.");
	}

}
