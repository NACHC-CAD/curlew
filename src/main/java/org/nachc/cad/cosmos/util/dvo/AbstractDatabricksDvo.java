package org.nachc.cad.cosmos.util.dvo;

import org.yaorma.dvo.Dvo;
import org.yaorma.util.string.DbToJavaNamingConverter;

import lombok.Getter;
import lombok.Setter;

// TODO: (JEG) GET RID OF THIS

@Getter
@Setter
public abstract class AbstractDatabricksDvo implements Dvo {

	@Override
	public String[] getJavaNames() {
		return DbToJavaNamingConverter.toJavaName(this.getColumnNames());
	}

	@Override
	public String[] getJavaNamesProper() {
		return DbToJavaNamingConverter.toJavaProperName(this.getColumnNames());
	}

	@Override
	public void addDescription(String javaName, String description) {
	}

	@Override
	public String getDescription(String javaName) {
		return null;
	}

}
