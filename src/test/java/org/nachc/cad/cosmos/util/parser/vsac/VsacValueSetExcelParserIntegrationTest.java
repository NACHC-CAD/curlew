package org.nachc.cad.cosmos.util.parser.vsac;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.StringWriter;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.Test;
import org.nachc.cad.cosmos.dvo.valueset.ValueSetDvo;

import com.nach.core.util.excel.ExcelUtil;
import com.nach.core.util.file.FileUtil;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VsacValueSetExcelParserIntegrationTest {

	@Test
	public void shouldParseExcel() {
		log.info("Starting test...");
		log.info("Getting file");
		File excelFile = getTestFile();
		log.info("Doing parse");
		VsacValueSetExcelParser parser = new VsacValueSetExcelParser();
		log.info("Doing parse");
		parser.parse(excelFile);
		String csv = parser.getCsvString();
		log.info("Parsed to csv: \n" + csv);
		assertTrue(csv.indexOf("HCPCS Level II") > 0);
		assertTrue(parser.getValueSet().getValueSetName().equals("Female or Male Condom Use"));
		log.info("Done.");
	}

	private File getTestFile() {
		File dir = FileUtil.getFile("/valueset/excel");
		List<File> files = FileUtil.listFiles(dir, "*.xlsx");
		return files.get(0);
	}

}
