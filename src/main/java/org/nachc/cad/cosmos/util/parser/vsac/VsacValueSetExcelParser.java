package org.nachc.cad.cosmos.util.parser.vsac;

import java.io.File;
import java.io.StringWriter;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.nachc.cad.cosmos.dvo.valueset.ValueSetDvo;

import com.nach.core.util.excel.ExcelUtil;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class VsacValueSetExcelParser {

	private String csvString;

	private ValueSetDvo valueSet;

	public void parse(File excelFile) {
		Workbook srcBook = ExcelUtil.getWorkbook(excelFile);
		parseExpansionList(srcBook);
		parseMetaData(srcBook);
	}

	private void parseExpansionList(Workbook srcBook) {
		// get the value set information from the "Value Set Info" sheet
		Sheet infoSheet = ExcelUtil.getSheet(srcBook, "Value Set Info");
		String valueSetName = ExcelUtil.getStringValue(infoSheet, 1, 1);
		String codeSystem = ExcelUtil.getStringValue(infoSheet, 2, 1);
		String oid = ExcelUtil.getStringValue(infoSheet, 3, 1);
		// get the sheet to read the value set list from
		Sheet srcSheet = srcBook.getSheet("Expansion List");
		// create the book and sheet to write to
		Workbook dstBook = ExcelUtil.createNewWorkbook();
		Sheet dstSheet = dstBook.createSheet("Expansion");
		// write the rows to the target sheet
		int srcRow = 12;
		int dstRow = 0;
		String code = ExcelUtil.getStringValue(srcSheet, 13, 0);
		while (StringUtils.isEmpty(code) == false) {
			Row row = dstSheet.createRow(dstRow);
			if(dstRow == 0) {
				ExcelUtil.setStringValue(dstSheet, "code", 0, 0);
				ExcelUtil.setStringValue(dstSheet, "description", 0, 1);
				ExcelUtil.setStringValue(dstSheet, "code_system", 0, 2);
				ExcelUtil.setStringValue(dstSheet, "code_system_version", 0, 3);
				ExcelUtil.setStringValue(dstSheet, "code_system_oid", 0, 4);
				ExcelUtil.setStringValue(dstSheet, "tty", 0, 5);
			} else {
				for (int c = 0; c < 6; c++) {
					row.createCell(c);
					String val = ExcelUtil.getStringValue(srcSheet, srcRow, c);
					ExcelUtil.setStringValue(dstSheet, val, dstRow, c);
				}
			}
			if (dstRow == 0) {
				ExcelUtil.setStringValue(dstSheet, "value_set_name", dstRow, 6);
				ExcelUtil.setStringValue(dstSheet, "code_system", dstRow, 7);
				ExcelUtil.setStringValue(dstSheet, "value_set_oid", dstRow, 8);
			} else {
				ExcelUtil.setStringValue(dstSheet, valueSetName, dstRow, 6);
				ExcelUtil.setStringValue(dstSheet, codeSystem, dstRow, 7);
				ExcelUtil.setStringValue(dstSheet, oid, dstRow, 8);
			}
			dstRow++;
			srcRow++;
			code = ExcelUtil.getStringValue(srcSheet, srcRow, 0);
		}
		// create the csv file
		StringWriter writer = new StringWriter();
		ExcelUtil.writeCsv(writer, dstSheet);
		this.csvString = writer.toString();
	}
	
	private void parseMetaData(Workbook srcBook) {
		Sheet sheet = srcBook.getSheet("Value Set Info");
		ValueSetDvo dvo = new ValueSetDvo();
		dvo.setValueSetName(ExcelUtil.getStringValue(sheet, 1, 1));
		dvo.setCodeSystem(ExcelUtil.getStringValue(sheet, 2, 1));
		dvo.setValueSetOid(ExcelUtil.getStringValue(sheet, 3, 1));
		dvo.setType(ExcelUtil.getStringValue(sheet, 4, 1));
		dvo.setDefinitionVersion(ExcelUtil.getStringValue(sheet, 5, 1));
		dvo.setSteward(ExcelUtil.getStringValue(sheet, 6, 1));
		dvo.setPurposeClinicalFocus(ExcelUtil.getStringValue(sheet, 10, 1));
		dvo.setPurposeDataElementScope(ExcelUtil.getStringValue(sheet, 11, 1));
		dvo.setPurposeInclusionCriteria(ExcelUtil.getStringValue(sheet, 12, 1));
		dvo.setPurposeExclusionCriteria(ExcelUtil.getStringValue(sheet, 13, 1));
		dvo.setNote(ExcelUtil.getStringValue(sheet, 14, 1));
		this.valueSet = dvo;
	}

	
	
}
