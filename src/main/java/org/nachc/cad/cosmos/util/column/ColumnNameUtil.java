package org.nachc.cad.cosmos.util.column;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.yaorma.util.string.DbToJavaNamingConverter;

import com.nach.core.util.csv.CsvUtil;
import com.nach.core.util.file.FileUtil;
import com.nach.core.util.string.StringUtil;

public class ColumnNameUtil {

	public static List<ColumnName> getColumnNames(File file) {
		return getColumnNames(file, ',');
	}

	public static List<ColumnName> getColumnNames(File file, char delim) {
		ArrayList<ColumnName> rtn = new ArrayList<ColumnName>();
		String headerLine = FileUtil.head(file, 1);
		headerLine = headerLine.trim();
		while (headerLine.endsWith(",")) {
			headerLine = headerLine.substring(0, headerLine.length() - 1);
		}
		List<String> columnNames = CsvUtil.parseLine(headerLine, delim);
		int cnt = 0;
		for (String str : columnNames) {
			cnt++;
			ColumnName name = new ColumnName();
			name.setColIndex(cnt);
			name.setDirtyName(str);
			name.setColName(getCleanName(str));
			rtn.add(name);
		}
		return rtn;
	}

	public static String getCleanName(String dirtyName) {
		String rtn = dirtyName;
		rtn = rtn.replace(".", "_");
		rtn = rtn.replace("-", "_");
		rtn = StringUtil.removeSpecial(rtn);
		rtn = DbToJavaNamingConverter.toDatabaseName(rtn);
		rtn = StringUtil.removeLeading(rtn, "_");
		return rtn;
	}

}
