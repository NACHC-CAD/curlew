package org.nachc.cad.cosmos.dvo.valueset;

import org.yaorma.dvo.Dvo;
import org.yaorma.util.string.DbToJavaNamingConverter;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ValueSetDvo implements Dvo {

	private String valueSetGuid;

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
				"value_set_guid",
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
	public String[] getPrimaryKeyColumnNames() {
		return new String[] { "value_set_guid" };
	}

	@Override
	public String[] getJavaNames() {
		return DbToJavaNamingConverter.toJavaName(this.getColumnNames());
	}

	@Override
	public String[] getJavaNamesProper() {
		return DbToJavaNamingConverter.toJavaProperName(this.getColumnNames());
	}

	@Override
	public String getTableName() {
		return "value_set.value_set";
	}

	@Override
	public void addDescription(String javaName, String description) {
	}

	@Override
	public String getDescription(String javaName) {
		return null;
	}

	@Override
	public String[] getPrimaryKeyValues() {
		return new String[] { this.valueSetGuid };
	}
}
