package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools.downloadfiles;

import java.io.File;
import java.io.InputStream;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.yaorma.database.Row;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;
import com.nach.core.util.file.FileUtil;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class DownloadFilesRunnable implements Runnable {

	private Row row;

	private File rootDir;
	
	private int threadId;
	
	private boolean success;
	
	private Exception exp;

	public DownloadFilesRunnable(Row row, File rootDir, int threadId) {
		this.row = row;
		this.rootDir = rootDir;
		this.threadId = threadId;
	}

	@Override
	public void run() {
		try {
			writeFile();
			this.success = true;
		} catch(Exception exp) {
			this.success = false;
			this.exp = exp;
		}
	}
	
	private void writeFile() {
		// get file location
		String url = row.get("fileLocation");
		url += "/" + row.get("fileName");
		// create target dir and file
		String orgCode = row.get("orgCode");
		String groupTableName = row.get("groupTableName");
		String fileName = row.get("fileName");
		String dirName = orgCode + "/" + groupTableName;
		File dir = new File(rootDir, dirName);
		File file = new File (dir, fileName);
		FileUtil.mkdirs(dir);
		String msg = "";
		// echo params
		msg += "------------------------\n";
		msg += "THREAD " + this.threadId + "\n";
		msg += "Reading file from: " + url + "\n";
		msg += "Writing file to: " + FileUtil.getCanonicalPath(file) + "\n";
		msg += "------------------------\n";
		log.info("\n" + msg);
		// get the file
		DatabricksFileUtil dbFiles = DatabricksFileUtilFactory.get();
		DatabricksFileUtilResponse resp = dbFiles.get(url);
		InputStream is = resp.getInputStream();
		// write the file
		dbFiles.writeLargeFile(url, file);
	}

}
