package org.nachc.cad.cosmos.dvo.cosmos;

import org.nachc.cad.cosmos.util.dvo.AbstractDatabricksDvo;
import org.yaorma.dvo.Dvo;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DocumentDvo extends AbstractDatabricksDvo {

	private String guid;

	private String documentName;

	private String documentSize;

	private String documentType;

	private String documentUseType;

	private String databricksPath;

	private String parentGuid;

	private String parentRelType;

	private String createdBy;

	private String createdDate;

	@Override
	public String[] getColumnNames() {
		String[] rtn = new String[] {
				"guid",
				"document_name",
				"document_size",
				"document_size_unit",
				"document_type",
				"document_use_type",
				"databricks_path",
				"parent_guid",
				"parent_rel_type",
				"created_by",
				"created_date"
		};

		return rtn;

	}

	@Override
	public String getSchemaName() {
		return "cosmos";
	}
	
	@Override
	public String getTableName() {
		return "document";
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
