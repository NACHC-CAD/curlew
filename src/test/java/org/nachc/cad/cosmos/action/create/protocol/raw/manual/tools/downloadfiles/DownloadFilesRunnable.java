package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools.downloadfiles;

import java.io.File;
import java.io.InputStream;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;
import org.yaorma.database.Row;
import org.yaorma.util.time.TimeUtil;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;
import com.nach.core.util.file.FileUtil;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class DownloadFilesRunnable implements Runnable {
	
	private static final int RETRY_COUNT = 20;

	private static final int SLEEP_SECONDS = 1;
	
	private Row row;

	private File rootDir;
	
	private int threadId;
	
	private boolean success = false;
	
	private Exception exp;

	private int currentAttempt = 0;
	
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
			if(this.currentAttempt > 0) {
				String msg = "";
				msg += "RETRYING:\n";
				msg += "!!!!!!!! SUCCESS !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
				msg += "ATTEMPT:   " + RETRY_COUNT + "\n";
				msg += "THREAD ID: " + this.threadId + "\n";
				msg += "FILE NAME: " + this.row.get("fileName") + "\n";
				msg += "ORG CODE   " + this.row.get("org") + "\n";
				msg += "TABLE:     " + this.row.get("groupTableName") + "\n";
				msg += "!!!!!!!! SUCCESS !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
				log.warn(msg);
			}
		} catch(Exception exp) {
			String msg = "";
			msg += "RETRYING:\n";
			msg += "-------------------------------------------\n";
			msg += "ATTEMPT:   " + RETRY_COUNT + "\n";
			msg += "THREAD ID: " + this.threadId + "\n";
			msg += "FILE NAME: " + this.row.get("fileName") + "\n";
			msg += "ORG CODE   " + this.row.get("org") + "\n";
			msg += "TABLE:     " + this.row.get("groupTableName") + "\n";
			msg += "-------------------------------------------\n";
			log.warn(msg);
			TimeUtil.sleep(SLEEP_SECONDS);
			currentAttempt++;
			if(currentAttempt <= RETRY_COUNT) {
				run();
			}
			this.exp = exp;
		}
	}
	
	private void writeFile() {
		// get file location
		String url = row.get("fileLocation");
		url += "/" + row.get("fileName");
		// create target dir and file
		String orgCode = row.get("orgCode");
		String providedDate = row.get("providedDate");
		String groupTableName = row.get("groupTableName");
		String fileName = row.get("fileName");
		//
		// DIR NAME
		//
		String dirName = orgCode + "/update-2022-02-1" + "/" + groupTableName;
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
