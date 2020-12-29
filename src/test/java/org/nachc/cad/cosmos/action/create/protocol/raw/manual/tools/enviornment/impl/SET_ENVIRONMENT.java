package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools.enviornment.impl;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.ConfirmConfiguration;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SET_ENVIRONMENT {

	public static final File RES = FileUtil.getFromProjectRoot("/src/main/resources");

	public static final File BUILD_RES = FileUtil.getFromProjectRoot("/target/classes");
	
	public static void main(String[] args) {
		String env = args[0];
		log.info("Setting environment to: " + env);
		if (env.equals("PROD")) {
			setToProd();
		} else if (env.equals("DEV")) {
			setToDev();
		} else {
			log.info("ENVIRONMENT NOT CHANGED");
		}
		ConfirmConfiguration.main(null);
		log.info("Done.");
	}

	private static void setToProd() {
		try {
			log.info("SETTING TO PROD");
			// copy to source folder
			copy(new File(RES, "_instances/prod/databricks.properties"), new File(RES, "databricks.properties"));
			copy(new File(RES, "auth/_instances/prod/databricks-auth.properties"), new File(RES, "auth/databricks-auth.properties"));
			copy(new File(RES, "auth/_instances/prod/mysql-auth.properties"), new File(RES, "auth/mysql-auth.properties"));
			// copy to classes file
			copy(new File(RES, "_instances/prod/databricks.properties"), new File(BUILD_RES, "databricks.properties"));
			copy(new File(RES, "auth/_instances/prod/databricks-auth.properties"), new File(BUILD_RES, "auth/databricks-auth.properties"));
			copy(new File(RES, "auth/_instances/prod/mysql-auth.properties"), new File(BUILD_RES, "auth/mysql-auth.properties"));
		} catch (Exception exp) {
			throw new RuntimeException(exp);
		}
	}

	private static void setToDev() {
		try {
			log.info("SETTING TO DEV");
			copy(new File(RES, "_instances/dev/databricks.properties"), new File(RES, "databricks.properties"));
			copy(new File(RES, "auth/_instances/dev/databricks-auth.properties"), new File(RES, "auth/databricks-auth.properties"));
			copy(new File(RES, "auth/_instances/dev/mysql-auth.properties"), new File(RES, "auth/mysql-auth.properties"));
			// copy to classes file
			copy(new File(RES, "_instances/dev/databricks.properties"), new File(BUILD_RES, "databricks.properties"));
			copy(new File(RES, "auth/_instances/dev/databricks-auth.properties"), new File(BUILD_RES, "auth/databricks-auth.properties"));
			copy(new File(RES, "auth/_instances/dev/mysql-auth.properties"), new File(BUILD_RES, "auth/mysql-auth.properties"));
		} catch (Exception exp) {
			throw new RuntimeException(exp);
		}
	}

	private static void copy(File src, File dst) {
		try {
			dst.delete();
			FileUtils.copyFile(src, dst);
			log.info("File copied:  ");
			log.info("Sourcce file: " + src.getCanonicalPath());
			log.info("Dst file:     " + dst.getCanonicalPath());
			log.info("Done copying databricks.properties");
		} catch (Exception exp) {
			throw new RuntimeException(exp);
		}
	}

}
