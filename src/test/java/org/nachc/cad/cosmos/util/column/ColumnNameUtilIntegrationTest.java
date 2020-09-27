package org.nachc.cad.cosmos.util.column;

import java.io.File;
import java.util.List;

import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ColumnNameUtilIntegrationTest {

	@Test
	public void shouldGetColumnNames() {
		log.info("Starting test...");
		File file = getFile();
		List<ColumnName> columnNames = ColumnNameUtil.getColumnNames(file);
		log.info("Got " + columnNames.size() + " columns");
		for(ColumnName col : columnNames) {
			log.info("\t" + col.getColIndex() + "\t" + col.getColName() + "\t\t" + col.getDirtyName());
		}
		log.info("Done.");
	}
	
	private File getFile() {
		String fileName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\current\\Womens Health\\demo\\thumb\\acdemo-thumbnail-10.csv";
		File file = new File(fileName);
		return file;
	}
	
}
