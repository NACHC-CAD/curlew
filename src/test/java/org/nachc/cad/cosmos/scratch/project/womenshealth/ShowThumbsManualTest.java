package org.nachc.cad.cosmos.scratch.project.womenshealth;

import java.io.File;
import java.util.List;

import org.junit.Test;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ShowThumbsManualTest {

	@Test
	public void generateThumbnails() {
		log.info("Starting test...");
		String dirName = "C:\\_WORKSPACES\\nachc\\_PROJECT\\current\\Womens Health";
		File root = new File(dirName);
		File dir = new File(root, "demo");
		log.info(getSectionBreak("DEMO FILES"));
		showThumbnails(dir);
		log.info("Done.");
	}

	private void showThumbnails(File dir) {
		List<File> files = FileUtil.listFiles(dir, "*.csv");
		for (File file : files) {
			String thumb = FileUtil.head(file, 10);
			log.info("FILE: " + file.getName() + "\n\n" + thumb + "\n");
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
