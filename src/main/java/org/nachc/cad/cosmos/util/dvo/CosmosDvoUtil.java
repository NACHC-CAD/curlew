package org.nachc.cad.cosmos.util.dvo;

import java.sql.Connection;
import java.util.Date;

import org.nachc.cad.cosmos.proxy.mysql.cosmos.PersonProxy;
import org.yaorma.database.Database;
import org.yaorma.dvo.Dvo;
import org.yaorma.util.time.TimeUtil;

import com.nach.core.util.guid.GuidFactory;
import com.nach.core.util.method.MethodUtil;

public class CosmosDvoUtil {
	
	public static void init(Dvo dvo, String createdByGuid) {
		String guid = GuidFactory.getGuid();
		Date now = TimeUtil.getNow();
		MethodUtil.set(dvo, "setGuid", guid);
		MethodUtil.set(dvo, "setCreatedBy", createdByGuid);
		MethodUtil.set(dvo, "setUpdatedBy", createdByGuid);
		MethodUtil.set(dvo, "setCreatedDate", now);
		MethodUtil.set(dvo, "setUpdatedDate", now);
	}
	
	public static void init(Dvo dvo, String uid, Connection conn) {
		String createdBy = PersonProxy.getGuidForUid(uid, conn);
		String guid = GuidFactory.getGuid();
		Date now = TimeUtil.getNow();
		MethodUtil.set(dvo, "setGuid", guid);
		MethodUtil.set(dvo, "setCreatedBy", createdBy);
		MethodUtil.set(dvo, "setUpdatedBy", createdBy);
		MethodUtil.set(dvo, "setCreatedDate", now);
		MethodUtil.set(dvo, "setUpdatedDate", now);
	}
	
}
