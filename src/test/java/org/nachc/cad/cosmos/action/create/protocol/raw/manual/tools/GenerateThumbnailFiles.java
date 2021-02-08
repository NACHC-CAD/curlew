package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools;

import java.io.File;
import java.util.List;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GenerateThumbnailFiles {

	public static final String SRC_DIR = "C:\\_WORKSPACES\\nachc\\_PROJECT\\cosmos\\million-hearts\\me-med-mapping-2021-02-02\\term\\";

	public static final Integer CNT = 250;

	public static void main(String[] args) {
		log.info("Starting...");
		File root = new File(SRC_DIR);
		List<File> dirs = FileUtil.listFiles(root);
		for (File file : dirs) {
			log.info("Processing dir: " + file.getName() + getSectionBreak(file.getName()));
			generateThumbnailsForDir(file);
		}
		log.info("Done.");
	}

	private static void generateThumbnailsForDir(File dir) {
		List<File> files = FileUtil.listFiles(dir, "*");
		File thumbDir = FileUtil.clearContents(dir, "thumb");
		for (File file : files) {
			String thumb = FileUtil.head(file, CNT);
			log.info("FILE: " + file.getName() + "\n\n" + thumb + "\n");
			String newFileName = FileUtil.getPrefix(file);
			String newFileSuffix = FileUtil.getSuffix(file);
			newFileName += "-thumbnail-" + CNT + newFileSuffix;
			File newFile = new File(thumbDir, newFileName);
			log.info("Writing file: " + FileUtil.getCanonicalPath(newFile));
			FileUtil.write(thumb, newFile);
			log.info("Done writing file.");
		}
	}

	private static String getSectionBreak(String name) {
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
