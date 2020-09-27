package org.nachc.cad.cosmos.util.column;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.yaorma.util.string.DbToJavaNamingConverter;

import com.nach.core.util.CsvUtil;
import com.nach.core.util.file.FileUtil;
import com.nach.core.util.string.StringUtil;

public class ColumnNameUtil {

	public static List<ColumnName> getColumnNames(File file) {
		ArrayList<ColumnName> rtn = new ArrayList<ColumnName>();
		String headerLine = FileUtil.head(file, 1);
		List<String> columnNames = CsvUtil.parseLine(headerLine);
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
		rtn = StringUtil.removeSpecial(rtn);
		rtn = DbToJavaNamingConverter.toDatabaseName(rtn);
		return rtn;
	}
	
}
