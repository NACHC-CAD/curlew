package org.nachc.cad.cosmos.dvo.valueset;

import org.nachc.cad.cosmos.util.dvo.AbstractDatabricksDvo;
import org.yaorma.dvo.Dvo;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ValueSetGroupMemberDvo extends AbstractDatabricksDvo implements Dvo {

	private String guid;
	
	private String valueSetGroupGuid;
	
	private String valueSetOid;
	
	private String valueSetVersion;
	
	private String valueSetName;
	
	private String codeSystem;
	
	@Override
	public String[] getColumnNames() {
		return new String[] {
				"guid",
				"value_set_group_guid",
				"value_set_oid",
				"value_set_version",
				"value_set_name",
				"code_system"
		};
	}

	@Override
	public String[] getPrimaryKeyColumnNames() {
		return new String[] {guid};
	}

	@Override
	public String[] getPrimaryKeyValues() {
		return new String[] {this.getGuid()};
	}

	@Override
	public String getSchemaName() {
		return "value_set";
	}

	@Override
	public String getTableName() {
		return "value_set_group_member";
	}

}
