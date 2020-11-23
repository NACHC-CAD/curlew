package org.nachc.cad.cosmos.util.guid;

import com.nach.core.util.guid.GuidFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GuidUtil {	

	public static void main(String[] args) {
		log.info("\n" + getGuid());
	}
	
	public static String getGuid() {
		return GuidFactory.getGuid();
	}
	
}
