package org.nachc.cad.cosmos.util.connection;

import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CosmosConnectionsIntegrationTest {

	@Test
	public void shouldGetConnections() {
		log.info("Starting test...");
		CosmosConnections conns = CosmosConnections.getConnections();
		conns.close();
		conns.close();
		log.info("Done.");
	}

}
