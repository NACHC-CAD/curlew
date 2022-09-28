package org.nachc.cad.cosmos.action.confirm;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConfigurationSummary {

	private String databricksDbInstance;
	
	private String databricksFileStoreInstance;

	private String mySqlDbInstance;
	
	private String javaVersion;
	
}
