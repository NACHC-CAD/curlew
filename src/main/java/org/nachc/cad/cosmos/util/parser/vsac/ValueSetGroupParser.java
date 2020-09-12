package org.nachc.cad.cosmos.util.parser.vsac;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.nachc.cad.cosmos.dvo.valueset.ValueSetGroupMemberDvo;
import org.yaorma.dvo.Dvo;

import com.nach.core.util.excel.ExcelUtil;
import com.nach.core.util.guid.GuidFactory;

import lombok.Getter;

@Getter
public class ValueSetGroupParser {

	private Workbook book;

	private Sheet sheet;

	private Sheet csvSheet;
	
	private String guid;

	public void init(File file, String guid) {
		this.book = ExcelUtil.getWorkbook(file);
		this.sheet = ExcelUtil.getSheet(book, 0);
		parse();
	}

	//
	// Method that does the actual parsing. 
	// The parsed csv is written to the csvSheet member variable.  
	//
	
	private void parse() {
		this.csvSheet = ExcelUtil.createNewWorkbook().createSheet("csv");
		int srcRow = 4;
		int dstRow = 0;
		while (StringUtils.isEmpty(ExcelUtil.getStringValue(sheet, srcRow, 5)) == false) {
			// get the values
			String oid = ExcelUtil.getStringValue(sheet, srcRow, 5);
			String version = ExcelUtil.getStringValue(sheet, srcRow, 6);
			String name = ExcelUtil.getStringValue(sheet, srcRow, 0);
			// set the values
			ExcelUtil.setStringValue(csvSheet, oid, dstRow, 0);
			ExcelUtil.setStringValue(csvSheet, version, dstRow, 1);
			ExcelUtil.setStringValue(csvSheet, name, dstRow, 2);
			srcRow++;
			dstRow++;
		}
	}
	
	public Sheet getCsvAsExcelSheet() {
		return this.csvSheet;
	}
	
	public List<ValueSetGroupMemberDvo> getAsDvoList() {
		List<ValueSetGroupMemberDvo> rtn = new ArrayList<ValueSetGroupMemberDvo>();
		Iterator<Row> rows = csvSheet.iterator();
		while(rows.hasNext()) {
			Row row = rows.next();
			ValueSetGroupMemberDvo dvo = new ValueSetGroupMemberDvo();
			dvo.setGuid(GuidFactory.getGuid());
			dvo.setValueSetGroupGuid(this.guid);
			dvo.setValueSetOid(ExcelUtil.getStringValue(row, 0));
			dvo.setValueSetVersion(ExcelUtil.getStringValue(row, 1));
			dvo.setValueSetName(ExcelUtil.getStringValue(row, 2));
			rtn.add(dvo);
		}
		return rtn;
	}

	public InputStream getCsvAsInputStream() {
		Sheet sheet = getCsvAsExcelSheet();
		StringWriter writer = new StringWriter();
		ExcelUtil.writeCsv(writer, sheet);
		String csvString = writer.toString();
		InputStream rtn = new ByteArrayInputStream(csvString.getBytes());
		return rtn;
	}

}
