package org.nachc.cad.cosmos.util.vsac;

import java.io.File;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.nach.core.util.excel.ExcelUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VsacValueSetParser {

	public static Sheet parseFile(File excelFile, File csvDir, Sheet meta) {
		if (meta == null) {
			Workbook metaBook = ExcelUtil.createNewWorkbook(false);
			meta = metaBook.createSheet("meta");
		}
		if (meta.getLastRowNum() <= 0) {
			Row row = meta.createRow(0);
			ExcelUtil.addCol(row, "value_set_name", 0);
			ExcelUtil.addCol(row, "code_system", 1);
			ExcelUtil.addCol(row, "value_set_oid", 2);
			ExcelUtil.addCol(row, "type", 3);
			ExcelUtil.addCol(row, "definition_version", 4);
			ExcelUtil.addCol(row, "steward", 5);
			ExcelUtil.addCol(row, "purpose_clinical_focus", 6);
			ExcelUtil.addCol(row, "purpose_data_element_scope", 7);
			ExcelUtil.addCol(row, "purpose_inclusion_criteria", 8);
			ExcelUtil.addCol(row, "purpose_exclusion_criteria", 9);
			ExcelUtil.addCol(row, "note", 10);
		}
		Workbook srcBook = ExcelUtil.getWorkbook(excelFile);
		Workbook dstBook = ExcelUtil.createNewWorkbook(false);
		// get the Value Set Info sheet
		Sheet infoSheet = ExcelUtil.getSheet(srcBook, "Value Set Info");
		// get the info values
		String valueSetName = ExcelUtil.getStringValue(infoSheet, 1, 1);
		String codeSystem = ExcelUtil.getStringValue(infoSheet, 2, 1);
		String oid = ExcelUtil.getStringValue(infoSheet, 3, 1);
		Sheet srcSheet = srcBook.getSheet("Expansion List");
		Sheet dstSheet = dstBook.createSheet("Expansion");
		int srcRow = 12;
		int dstRow = 0;
		String code = ExcelUtil.getStringValue(srcSheet, 13, 0);
		while (StringUtils.isEmpty(code) == false) {
			Row row = dstSheet.createRow(dstRow);
			log.info("dstRow: " + dstRow + "\tROW: " + row.getRowNum() + "\tCODE: " + code);
			for (int c = 0; c < 5; c++) {
				row.createCell(c);
				String val = ExcelUtil.getStringValue(srcSheet, srcRow, c);
				ExcelUtil.setStringValue(dstSheet, val, dstRow, c);
			}
			row.createCell(5);
			row.createCell(6);
			row.createCell(7);
			if (dstRow == 0) {
				ExcelUtil.setStringValue(dstSheet, "value_set_name", dstRow, 5);
				ExcelUtil.setStringValue(dstSheet, "code_system", dstRow, 6);
				ExcelUtil.setStringValue(dstSheet, "oid", dstRow, 7);
			} else {
				ExcelUtil.setStringValue(dstSheet, valueSetName, dstRow, 5);
				ExcelUtil.setStringValue(dstSheet, codeSystem, dstRow, 6);
				ExcelUtil.setStringValue(dstSheet, oid, dstRow, 7);
			}
			dstRow++;
			srcRow++;
			code = ExcelUtil.getStringValue(srcSheet, srcRow, 0);
		}
		// create the csv file
		File csvFile = new File(csvDir + "/" + excelFile.getName() + ".csv");
		ExcelUtil.saveAsCsv(dstSheet, csvFile);
		// add to the metadata file
		Sheet metaSheet = srcBook.getSheet("Value Set Info");
		Row row = ExcelUtil.createNextRow(meta);
		int rowCnt = row.getRowNum();
		log.debug("Added row to meta: " + rowCnt);
		// add metadata
		ExcelUtil.addCol(row, ExcelUtil.getStringValue(metaSheet, 1, 1), 0);
		ExcelUtil.addCol(row, ExcelUtil.getStringValue(metaSheet, 2, 1), 1);
		ExcelUtil.addCol(row, ExcelUtil.getStringValue(metaSheet, 3, 1), 2);
		ExcelUtil.addCol(row, ExcelUtil.getStringValue(metaSheet, 4, 1), 3);
		ExcelUtil.addCol(row, ExcelUtil.getStringValue(metaSheet, 5, 1), 4);
		ExcelUtil.addCol(row, ExcelUtil.getStringValue(metaSheet, 6, 1), 5);
		ExcelUtil.addCol(row, ExcelUtil.getStringValue(metaSheet, 10, 1), 6);
		ExcelUtil.addCol(row, ExcelUtil.getStringValue(metaSheet, 11, 1), 7);
		ExcelUtil.addCol(row, ExcelUtil.getStringValue(metaSheet, 12, 1), 8);
		ExcelUtil.addCol(row, ExcelUtil.getStringValue(metaSheet, 13, 1), 9);
		ExcelUtil.addCol(row, ExcelUtil.getStringValue(metaSheet, 14, 1), 10);
		log.info("* * *");
		log.info(ExcelUtil.getStringValue(meta, 0, 0));
		log.info("* * *");
		return meta;
	}

	public static void writeMetaData(File dir, Sheet sheet) {
		File file = new File(dir, "meta.csv");
		if (file.exists()) {
			file.delete();
		}
		ExcelUtil.saveAsCsv(sheet, file);
	}

}
