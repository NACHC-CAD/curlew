package org.nachc.cad.cosmos.action.delete;

import org.nachc.cad.cosmos.util.connection.CosmosConnections;
import org.yaorma.database.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteProjectAction {

	public static void delete(String projectCode, CosmosConnections conns) {
		deleteUrls(projectCode, conns);
		deleteProject(projectCode, conns);
		deleteProjectCode(projectCode, conns);
	}
	
	private static void deleteUrls(String projectCode, CosmosConnections conns) {
		log.info("Deleting project urls records.");
		int cnt = Database.update("delete from proj_url where project = ?", projectCode , conns.getMySqlConnection());
		log.info(cnt + " PROJ_URL Records deleted.");
	}
	
	private static void deleteProject(String projectCode, CosmosConnections conns) {
		log.info("Deleting project records.");
		int cnt = Database.update("delete from project where code = ?", projectCode , conns.getMySqlConnection());
		log.info(cnt + " PROJECT Records deleted.");
	}
	
	private static void deleteProjectCode(String projectCode, CosmosConnections conns) {
		log.info("Deleting proj_code records.");
		int cnt = Database.update("delete from proj_code where code = ?", projectCode , conns.getMySqlConnection());
		log.info(cnt + " PROJ_CODE Records deleted.");
	}
	
}
