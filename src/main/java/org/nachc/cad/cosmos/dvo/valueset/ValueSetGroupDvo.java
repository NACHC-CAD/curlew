package org.nachc.cad.cosmos.dvo.valueset;

import java.util.Date;

import org.nachc.cad.cosmos.util.dvo.AbstractDatabricksDvo;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ValueSetGroupDvo extends AbstractDatabricksDvo {

	private String guid;

	private String name;

	private String type;

	private String description;
	
	private String createdBy;

	private Date createdDate;

	@Override
	public String[] getColumnNames() {
		return new String[] {
				"guid",
				"name",
				"type",
				"description",
				"created_by",
				"created_date"
		};
	}

	@Override
	public String[] getPrimaryKeyColumnNames() {
		return new String[] {"guid"};
	}

	@Override
	public String[] getPrimaryKeyValues() {
		return new String[] { getGuid() };
	}

	@Override
	public String getSchemaName() {
		return "value_set";
	}

	@Override
	public String getTableName() {
		return "value_set_group";
	}

}
