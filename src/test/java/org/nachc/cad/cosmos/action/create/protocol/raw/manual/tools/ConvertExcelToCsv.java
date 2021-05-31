package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import java.io.File;
import java.util.List;

import com.nach.core.util.excel.ExcelUtil;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConvertExcelToCsv {
	
	private static final String DIR = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\covid\\update-2021-05-27-COVID-HCN\\_ETC";
	
	private static final String PAT = "**/*.xlsx";
	
	public static void main(String[] args) {
		List<File> files = FileUtil.listFiles(DIR, PAT);
		for(File file : files) {
			log.info("Processing: " + file.getName());
			ExcelUtil.saveAsCsv(file);
		}
	}
	
}
