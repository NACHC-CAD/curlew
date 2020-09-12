package org.nachc.cad.cosmos.util.dao;

import java.io.File;
import java.sql.Connection;

import org.nachc.cad.cosmos.proxy.cosmos.DocumentProxy;
import org.nachc.cad.cosmos.util.dao.result.DocumentDaoResult;
import org.yaorma.dao.Dao;
import org.yaorma.dvo.Dvo;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DocumentDao {

	// TODO: FINISH THIS THOUGHT
	
	public static DocumentDaoResult uploadDocument(File file, DocumentProxy dvo, Connection conn, DatabricksFileUtil fileUtil) {
		DocumentDaoResult result = new DocumentDaoResult();
		return result;
	}

	private static void writeDatabaseRecord(Dvo dvo, Connection conn) {
		Dao.doDatabricksInsert(dvo, conn);
	}

	private static DatabricksFileUtilResponse writeFile(File file, DocumentProxy dvo, DatabricksFileUtil fileUtil) {
		String dir = dvo.getDatabricksPath();
		String fileName = "COSMOS_" + dvo.getGuid() + "_" + dvo.getDocumentName();
		String path = dir + "/" + fileName;
		DatabricksFileUtilResponse resp = fileUtil.replace(path, file);
		return resp;
	}
	
}
