package org.nachc.cad.cosmos.action.create.protocol.raw.params;

import java.io.File;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CreateProtocolRawDataParams {

	private String protocolName;

	private String protocolNamePretty;

	private String dataGroupName;

	private String dataGroupAbr;

	private String databricksFileLocation;
	
	private String databricksFileName;

	private File file;

	private String createdBy;
	
	public String getDatabricksFilePath() {
		return databricksFileLocation + "/" + databricksFileName;
	}

}
