package org.nachc.cad.cosmos.mysql.update.codegenerator;

import java.io.File;
import java.sql.Connection;

import org.nachc.cad.cosmos.util.mysql.connection.MySqlConnectionFactory;
import org.yaorma.codeGenerator.generateOrmForSchema.GenerateOrmForSchema;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GenerateCosmosDvos {

	public static void main(String[] args) {
		generateDvos();
	}

	public static void generateDvos() {
		try {
			Connection conn = MySqlConnectionFactory.getCosmosConnection();
			String schemaName = "cosmos";
			String packageName = "org.nachc.cad.cosmos.dvo.mysql.cosmos";
			File destDir = FileUtil.getFromProjectRoot("/src/main/java/org/nachc/cad/cosmos/dvo/mysql/cosmos");
			FileUtil.clearContents(destDir);
			GenerateOrmForSchema.execute(conn, schemaName, packageName, destDir);
			log.info("Done with generate dvos.");
		} catch(Exception exp) {
			throw new RuntimeException(exp);
		}
	}
	
}
