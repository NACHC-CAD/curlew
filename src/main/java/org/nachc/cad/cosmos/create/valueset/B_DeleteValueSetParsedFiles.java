package org.nachc.cad.cosmos.create.valueset;

import java.io.File;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class B_DeleteValueSetParsedFiles {

	public static void deleteFiles() {
		try {
			File file;
			// delete csv files
			file = new File(A_ParametersForValueSetSchema.CSV_FILE_ROOT);
			log.info("Clearing contents of: " + A_ParametersForValueSetSchema.CSV_FILE_ROOT);
			FileUtil.clearContents(file);
			log.info("File exists: " + file.exists() + "\t" + file.getCanonicalPath());
			// delete meta file
			file = new File(A_ParametersForValueSetSchema.META_FILE_ROOT);
			log.info("Clearing contents of: " + file.getCanonicalFile());
			FileUtil.clearContents(file);
			log.info("File exists: " + file.exists() + "\t" + file.getCanonicalPath());
			// done
			log.info("Done deleting files");
		} catch (Exception exp) {
			throw new RuntimeException(exp);
		}
	}

}
