package org.nachc.cad.cosmos.create.valueset;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.nachc.cad.cosmos.util.databricks.database.DatabricksFileUtilFactory;

import com.nach.core.util.databricks.file.DatabricksFileUtil;
import com.nach.core.util.databricks.file.response.DatabricksFileUtilResponse;
import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class F_PostValueSetFilesToDatabricks {

	public static void main(String[] args) {
		post();
	}

	public static void post() {
		postCsvFiles();
		postMeta();
	}

	public static void postCsvFiles() {
		String databricksTargetDir = A_ParametersForValueSetSchema.DATABRICKS_FILE_PATH;
		File csvFilesDir = new File(A_ParametersForValueSetSchema.CSV_FILE_ROOT);
		List<File> files = FileUtil.listFiles(csvFilesDir, "*.csv");
		int cnt = 0;
		ArrayList<DatabricksFileUtilResponse> responseList = new ArrayList<DatabricksFileUtilResponse>();
		for (File file : files) {
			cnt++;
			log.info("\tFile " + cnt + " of " + files.size() + ": " + file.getName());
			log.info("\tTarget: " + databricksTargetDir);
			DatabricksFileUtilResponse resp = DatabricksFileUtilFactory.get().put(databricksTargetDir, file);
			responseList.add(resp);
		}
		cnt = 0;
		for (DatabricksFileUtilResponse resp : responseList) {
			cnt++;
			log.info(cnt + " of " + responseList.size() + ":\t" + resp.isSuccess() + "\t" + resp.getStatusCode() + "\t" + resp.getResponse().trim());
		}
		log.info("Done.");
	}

	private static void postMeta() {
		try {
			String metaFileDatabricksTargetDir = A_ParametersForValueSetSchema.DATABRICKS_META_FILE_PATH;
			File meta = new File(A_ParametersForValueSetSchema.META_FILE_ROOT, "meta.csv");
			log.info("File: " + meta.getCanonicalPath());
			log.info("Path: " + metaFileDatabricksTargetDir);
			DatabricksFileUtilResponse resp = DatabricksFileUtilFactory.get().put(metaFileDatabricksTargetDir, meta);
			log.info(resp.isSuccess() + "\t" + resp.getStatusCode() + "\t" + resp.getResponse().trim());
			log.info("Done uploading meta.");
		} catch (Exception exp) {
			throw new RuntimeException(exp);
		}
	}

}
