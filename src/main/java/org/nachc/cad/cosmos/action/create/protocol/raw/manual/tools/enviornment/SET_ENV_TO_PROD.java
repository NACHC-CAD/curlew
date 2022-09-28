package org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools.enviornment;

import org.nachc.cad.cosmos.action.create.protocol.raw.manual.tools.enviornment.impl.SET_ENVIRONMENT;

public class SET_ENV_TO_PROD {

	public static void main(String[] args) {
		exec();
	}

	public static void exec() {
		SET_ENVIRONMENT.main(new String[] {"PROD"});
	}
	
}
