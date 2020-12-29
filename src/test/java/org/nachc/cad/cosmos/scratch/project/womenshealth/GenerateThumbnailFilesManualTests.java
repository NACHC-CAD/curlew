package org.nachc.cad.cosmos.scratch.project.womenshealth;

import java.io.File;
import java.util.List;

import org.junit.Test;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GenerateThumbnailFilesManualTests {

	@Test
	public void generateThumbnails() {
		log.info("Starting test...");
		String dirName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\current\\Womens Health";
		File root = new File(dirName);
		List<File> dirs = FileUtil.listFiles(root);
		for (File file : dirs) {
			if ("_txt".contentEquals(file.getName())) {
				continue;
			}
			log.info("Processing dir: " + file.getName() + getSectionBreak(file.getName()));
			generateThumbnailsForDir(file);
		}
		log.info("Done.");
	}

	private void generateThumbnailsForDir(File dir) {
		List<File> files = FileUtil.listFiles(dir, "*.csv");
		File thumbDir = FileUtil.clearContents(dir, "thumb");
		for (File file : files) {
			String thumb = FileUtil.head(file, 10);
			log.info("FILE: " + file.getName() + "\n\n" + thumb + "\n");
			String newFileName = FileUtil.getPrefix(file);
			newFileName += "-thumbnail-10.csv";
			File newFile = new File(thumbDir, newFileName);
			log.info("Writing file: " + FileUtil.getCanonicalPath(newFile));
			FileUtil.write(thumb, newFile);
			log.info("Done writing file.");
		}
	}

	private String getSectionBreak(String name) {
		String rtn = "";
		rtn += "\n";
		rtn += "\n* * * * * * * * * * * * * * * * * * ";
		rtn += "\n* ";
		rtn += "\n* GETTING FILES: " + name;
		rtn += "\n* ";
		rtn += "\n* * * * * * * * * * * * * * * * * * ";
		rtn += "\n";
		return rtn;
	}

}
