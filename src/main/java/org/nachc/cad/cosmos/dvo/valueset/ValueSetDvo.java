package org.nachc.cad.cosmos.dvo.valueset;

import org.nachc.cad.cosmos.util.dvo.AbstractDatabricksDvo;
import org.yaorma.dvo.Dvo;
import org.yaorma.util.string.DbToJavaNamingConverter;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ValueSetDvo extends AbstractDatabricksDvo {

	private String guid;

	private String valueSetName;

	private String codeSystem;

	private String valueSetOid;

	private String type;

	private String definitionVersion;

	private String steward;

	private String purposeClinicalFocus;

	private String purposeDataElementScope;

	private String purposeInclusionCriteria;

	private String purposeExclusionCriteria;

	private String note;

	@Override
	public String[] getColumnNames() {
		return new String[] {
				"guid",
				"value_set_name",
				"code_system",
				"value_set_oid",
				"type",
				"definition_version",
				"steward",
				"purpose_clinical_focus",
				"purpose_data_element_scope",
				"purpose_inclusion_criteria",
				"purpose_exclusion_criteria",
				"note"
		};
	}

	@Override
	public String getSchemaName() {
		return "value_set";
	}
	
	@Override
	public String getTableName() {
		return "value_set";
	}

	@Override
	public String[] getPrimaryKeyColumnNames() {
		return new String[] { "guid" };
	}

	@Override
	public String[] getPrimaryKeyValues() {
		return new String[] { this.guid };
	}

}
