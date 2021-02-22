package org.nachc.cad.cosmos.action.create.protocol.raw.databricks;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.RawTableFileDvo;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.dao.Dao;
import org.yaorma.database.Database;

import com.nach.core.util.excel.ExcelUtil;
import com.nach.core.util.string.StringUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateColumnMappingsAction {

	public static void exec(File srcFile, CosmosConnections conns) {
		Workbook book = ExcelUtil.getWorkbook(srcFile);
		List<Sheet> sheets = ExcelUtil.getSheets(book);
		for (Sheet sheet : sheets) {
			if ("FILES".equals(sheet.getSheetName()) == false) {
				processSheet(sheet, conns);
			}
		}
	}

	private static void processSheet(Sheet sheet, CosmosConnections conns) {
		Iterator<Row> rows = ExcelUtil.getRows(sheet);
		// skip header row
		rows.next();
		while (rows.hasNext()) {
			Row row = rows.next();
			String fileName = ExcelUtil.getStringValue(row, 1);
			String colName = ExcelUtil.getStringValue(row, 5);
			String colAlias = ExcelUtil.getStringValue(row, 6);
			if(colAlias != null && StringUtil.isEmpty(colAlias) == false) {
				log.info("\tUPDATING COL: " + colName + "|" + colAlias + "|" + fileName);
				String rawTableGuid = getRawTableGuid(fileName, conns);
				String sqlString = "update raw_table_col set col_alias = ? where col_name = ? and raw_table = ?";
				String[] params = {colAlias, colName, rawTableGuid};
				int cnt = Database.update(sqlString,  params, conns.getMySqlConnection());
				if(cnt > 1) {
					log.info("\tRecords Updated: " + cnt);
					conns.rollback();
					throw new RuntimeException("To many records updated: " + cnt);
				}
			}
		}
	}

	private static String getRawTableGuid(String fileName, CosmosConnections conns) {
		RawTableFileDvo dvo = Dao.find(new RawTableFileDvo(), "file_name", fileName, conns.getMySqlConnection());
		String guid = dvo.getRawTable();
		return guid;
	}

}
