package org.nachc.cad.cosmos.init.womenshealth;

import java.io.File;
import java.util.List;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateThumbNails {

	private static final String ROOT = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\womens-health\\thumb";
	
	public static void main(String[] args) {
		log.info("Starting main...");
		File root = new File(ROOT);
		List<File> files = FileUtil.listFiles(root, "*/**/*");
		log.info("Got " + files.size() + " files.");
		for(File file : files) {
			createThumb(file);
		}
		log.info("Done.");
	}
	
	private static void createThumb(File file) {
		String prefix = "THUBMNAIL_10_";
		if(file.getName().startsWith(prefix)) {
			return;
		} else {
			String thumb = FileUtil.head(file, 10);
			File thumbDir = file.getParentFile();
			log.info("FILE: " + file.getName() + "\n\n" + thumb + "\n");
			String newFileName = file.getName();
			newFileName = prefix + newFileName;
			File newFile = new File(thumbDir, newFileName);
			if(newFile.exists()) {
				newFile.delete();
			}
			log.info("Writing file: " + FileUtil.getCanonicalPath(newFile));
			FileUtil.write(thumb, newFile);
			file.delete(); 
		}
		log.info("Done writing file.");
	}
	
}
