package org.nachc.cad.cosmos.action.create.protocol.raw.manual.build.project.covid;

import java.io.File;
import java.util.List;

import org.junit.Test;
import org.nachc.cad.cosmos.action.delete.DeleteProjectAction;
import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.nachc.cad.cosmos.util.params.DatabricksParams;
import org.nachc.cad.cosmos.util.project.UploadDir;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BuildCovidUsingDir {

	public static final String PROJ_NAME = "covid";

	public static final String UID = "greshje";

	public static final String ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\curlew\\covid";

	/**
	 * 
	 * Delete existing project and create a new project from data files.
	 * 
	 */

	public static void main(String[] args) {
		CosmosConnections conns = CosmosConnections.getConnections();
		try {
			log.info("Starting test...");
			// delete the project if it exists
//			log.info("Deleting existing project");
//			DeleteProjectAction.exec(PROJ_NAME, DatabricksParams.getProjectFilesRoot(), conns);
			// built the project
			log.info("Uploading files from dirs:");
			List<File> files = getFiles();
			files = FileUtil.removeStartsWith(files, "_");
			for(File file : files) {
				log.info("  " + file);
			}
			for (File file : files) {
				log.info("* * * ---------------------");
				log.info("* * *");
				log.info("* * *");
				log.info("* * * UPLOADING FROM DIR: " + file.getName());
				log.info("* * *");
				log.info("* * *");
				log.info("* * * ---------------------");
				UploadDir.uploadDir(file, UID, conns, true);
				conns.commit();
			}
			// commit
			conns.commit();
		} finally {
			conns.close();
		}
		log.info("Done.");
	}

	private static List<File> getFiles() {
		List<File> files = FileUtil.list(new File(ROOT));
		FileUtil.removeStartsWith(files, "_");
		return files;
	}

}
