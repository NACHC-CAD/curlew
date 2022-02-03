package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools.downloadfiles;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.database.Data;
import org.yaorma.database.Database;
import org.yaorma.database.Row;

import com.nach.core.util.file.FileUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DownloadFilesTool {

	private static final File ROOT_DIR = new File("C:\\temp-databricks-download\\databricks-download");

	private static ArrayList<DownloadFilesRunnable> runnables = new ArrayList<DownloadFilesRunnable>();
	
	private static ArrayList<Thread> threads = new ArrayList<Thread>();

	public static void main(String[] args) {
		String sqlString = getFilesSqlString();
		exec(sqlString);
	}

	public static void exec(String sqlString) {
		log.info("Doing delete...\n\nDeleting " + FileUtil.getCanonicalPath(ROOT_DIR) + "\n\n");
		FileUtil.rmdir(ROOT_DIR);
		log.info("Getting connections...");
		CosmosConnections conns = new CosmosConnections();
		try {
			log.info("Getting files...");
			Data data = getFiles(sqlString, conns);
			log.info("Got " + data.size() + " files");
			int cnt = 0;
			for (Row row : data) {
				cnt++;
				DownloadFilesRunnable runnable = new DownloadFilesRunnable(row, ROOT_DIR, cnt);
				runnables.add(runnable);
				Thread thread = new Thread(runnable);
				threads.add(thread);
			}
			threads.get(0).run();
			for (Thread thread : threads) {
				thread.start();
			}
			for (Thread thread : threads) {
				try {
					thread.join();
				} catch (Exception exp) {
					log.warn("COULD NOT JOIN THREAD");
				}
			}
			String missing = "\n\n\n";
			String present = "\n\n\n";
			for(DownloadFilesRunnable runnable : runnables) {
				Row row = runnable.getRow();
				if(runnable.isSuccess() == false) {
					missing += "\t" + row.get("orgCode");
					missing += "\t" + row.get("groupTableName");
					missing += "\t" + row.get("dataLot");
					missing += "\t" + row.get("fileName"); 
					missing += "\n";
				} else {
					present += "\t" + row.get("orgCode");
					present += "\t" + row.get("groupTableName");
					present += "\t" + row.get("dataLot");
					present += "\t" + row.get("fileName"); 
					present += "\n";
				}
			}
			missing += "\n\n";
			present += "\n\n";
			log.info("FILES THAT COULD NOT BE DOWNLOADED: " + missing);			
			log.info("FILES THAT COULD BE DOWNLOADED: " + present);			
			log.info("Done.");
		} finally {
			conns.close();
		}
	}
	
	private static Data getFiles(String sqlString, CosmosConnections conns) {
		Connection conn = conns.getMySqlConnection();
		Data data = Database.query(sqlString, conn);
		return data;
	}

	private static String getFilesSqlString() {
		String sqlString = "";
		sqlString += "select distinct \n";
		sqlString += "	project, \n";
		sqlString += "    group_table_name, \n";
		sqlString += "    org_code, \n";
		sqlString += "    data_lot, \n";
		sqlString += "    file_location, \n";
		sqlString += "    file_name, \n";
		sqlString += "    file_size, \n";
		sqlString += "    file_size_units, \n";
		sqlString += "    provided_by, \n";
		sqlString += "    provided_date, \n";
		sqlString += "    file_created_date \n";
		sqlString += "from \n";
		sqlString += "	raw_table_col_detail \n";
		sqlString += "where \n";
		sqlString += "	project = 'womens_health' \n";
		sqlString += "  and data_lot != 'LOT 1' \n";
		sqlString += "and \n";
		sqlString += "	org_code in ('ac','ochin') \n";
		sqlString += "order by project, org_code, group_table_name, file_name \n";
		return sqlString;
	}

}
