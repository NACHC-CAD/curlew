package org.nachc.cad.cosmos.proxy.cosmos;

import java.io.File;
import java.sql.Connection;
import java.util.Date;

import org.nachc.cad.cosmos.dvo.cosmos.DocumentDvo;
import org.nachc.cad.cosmos.util.databricks.auth.DatabricksAuthUtil;
import org.nachc.cad.cosmos.util.params.DatabricksParams;
import org.yaorma.dao.Dao;
import org.yaorma.dvo.DvoUtil;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.exception.DatabricksFileException;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DocumentProxy extends DocumentDvo {

	private File file;
	
	public void setFile(File file) {
		this.file = file;
		this.setDocumentSize(FileUtil.size(file));
		this.setDocumentName(file.getName());
		this.setDocumentSizeUnit("B");
	}
	
	public void saveToDatabricks(Connection conn) {
		saveFile();
		saveDatabaseRecord(conn);
	}

	private void saveFile() {
		if(fileExists()) {
			deleteFile();
		}
		writeFile();
	}
	
	private boolean fileExists() {
		log.info("Looking for file on databricks...");
		String url = DatabricksParams.getRestUrl();
		String token = DatabricksAuthUtil.getToken();
		DatabricksFileUtil util = new DatabricksFileUtil(url, token);
		DatabricksFileUtilResponse resp = util.exists(this.getDatabricksPath() + "/" + this.getDocumentName());
		if(resp.isSuccess() == false) {
			log.info("Exception looking for existing file:\n");
			resp.getResponse();
			throw new DatabricksFileException(resp);
		}
		boolean exists = resp.isFileExists();
		return exists;
	}
	
	private boolean deleteFile() {
		log.info("Deleting file from databricks...");
		String url = DatabricksParams.getRestUrl();
		String token = DatabricksAuthUtil.getToken();
		DatabricksFileUtil util = new DatabricksFileUtil(url, token);
		DatabricksFileUtilResponse resp = util.delete(this.getDatabricksPath() + "/" + this.getDocumentName());
		if(resp.isSuccess() == false) {
			log.info("Exception deleting existing file:\n");
			resp.getResponse();
			throw new DatabricksFileException(resp);
		}
		boolean exists = resp.isFileExists();
		return exists;
	}
	
	private void writeFile() {
		log.info("Writing file to databricks...");
		String url = DatabricksParams.getRestUrl();
		String token = DatabricksAuthUtil.getToken();
		DatabricksFileUtil util = new DatabricksFileUtil(url, token);
		DatabricksFileUtilResponse resp = util.put(this.getDatabricksPath(), this.file);
		if(resp.isSuccess() == false) {
			log.info("Exception writing file:\n");
			resp.getResponse();
			throw new DatabricksFileException(resp);
		}
	}
	
	private void saveDatabaseRecord(Connection conn) {
		log.info("Saving database record");
		Dao.insertUsingLiteral(this, conn);
		log.info("Done saving database record.");
	}

}


