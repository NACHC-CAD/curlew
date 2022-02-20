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
import org.yaorma.database.Data;
import org.yaorma.database.Database;

import com.nach.core.util.excel.ExcelUtil;
import com.nach.core.util.string.StringUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateColumnMappingsAction {

	public static void exec(File mappingFile, CosmosConnections conns) {
		exec(mappingFile, null, conns);
	}

	public static void exec(File mappingFile, File dataFile, CosmosConnections conns) {
		Workbook book = ExcelUtil.getWorkbook(mappingFile);
		List<Sheet> sheets = ExcelUtil.getSheets(book);
		for (Sheet sheet : sheets) {
			if ("FILES".equals(sheet.getSheetName()) == false) {
				processSheet(sheet, dataFile, conns);
			}
		}
	}

	private static void processSheet(Sheet sheet, File dataFile, CosmosConnections conns) {
		Iterator<Row> rows = ExcelUtil.getRows(sheet);
		// skip header row
		rows.next();
		while (rows.hasNext()) {
			Row row = rows.next();
			String fileName;
			if (dataFile == null) {
				fileName = ExcelUtil.getStringValue(row, 1);
			} else {
				fileName = dataFile.getName();
			}
			String colName = ExcelUtil.getStringValue(row, 5);
			String colAlias = ExcelUtil.getStringValue(row, 6);
			String projectName = ExcelUtil.getStringValue(row, 2);
			if (colAlias != null && StringUtil.isEmpty(colAlias) == false) {
				log.info("\tUPDATING COL: " + colName + "|" + colAlias + "|" + fileName);
				String rawTableGuid = getRawTableGuid(fileName, projectName, conns);
				if (rawTableGuid == null) {
					continue;
				}
				String sqlString = "update raw_table_col set col_alias = ? where col_name = ? and raw_table = ?";
				String[] params = { colAlias, colName, rawTableGuid };
				int cnt = Database.update(sqlString, params, conns.getMySqlConnection());
				if (cnt > 1) {
					log.info("\tRecords Updated: " + cnt);
					conns.rollback();
					throw new RuntimeException("To many records updated: " + cnt);
				}
				if (cnt == 1) {
					log.info("\t--------------------------------------------");
					log.info("\tLOG: " + colName + " changed to " + colAlias);
					log.info("\tFOR FILE: (" + rawTableGuid + ") " + fileName);
					log.info("\tFOR PROJ: " + projectName);
					log.info("\t--------------------------------------------");
				}
				if (cnt == 0) {
					log.info("\txxx");
					log.info("\tLOG: " + colName + " NOT changed to " + colAlias);
					log.info("\tFOR FILE: (" + rawTableGuid + ") " + fileName);
					log.info("\tFOR PROJ: " + projectName);
					log.info("\txxx");
				}
			}
		}
	}

	private static String getRawTableGuid(String fileName, String projectName, CosmosConnections conns) {
		if (projectName != null && projectName.trim().length() > 0) {
			projectName = projectName.trim();
			String sqlString = "select * from raw_table_file where file_name = ? and project = ?";
			String[] params = { fileName, projectName };
			Data data = Database.query(sqlString, params, conns.getMySqlConnection());
			if(data.size() > 0) {
				return data.get(0).get("rawTable");
			} else {
				return null;
			}
			
		} else {
			RawTableFileDvo dvo = Dao.find(new RawTableFileDvo(), "file_name", fileName, conns.getMySqlConnection());
			if (dvo == null) {
				return null;
			} else {
				String guid = dvo.getRawTable();
				return guid;
			}
		}
	}

}
