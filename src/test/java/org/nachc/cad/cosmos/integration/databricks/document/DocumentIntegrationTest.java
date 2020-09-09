package org.nachc.cad.cosmos.integration.databricks.document;

import org.junit.Test;
import org.nachc.cad.cosmos.dvo.cosmos.DocumentDvo;

import com.nach.core.util.guid.GuidFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DocumentIntegrationTest {

	@Test
	public void shouldGetResponse() {
		log.info("Starting test...");
		DocumentDvo dvo = new DocumentDvo();
		String guid = GuidFactory.getGuid();
		dvo.setGuid(guid);
		dvo.setDocumentUseType("INTEGRATION_TEST");
		dvo.setCreatedBy("greshje");
		
		log.info("Done.");
	}
	
}
