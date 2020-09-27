//
// Data Value Object (DVO) for foo
//

package org.nachc.cad.cosmos.dvo.mysql.cosmos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

import org.yaorma.dvo.Dvo;

public class FooDvo implements Dvo {

    //
    // tableName
    //
    
    public static final String TABLE_NAME = "foo";
    
    //
    // schemaName
    //
    
    public static final String SCHEMA_NAME = "cosmos";
    
    //
    // columnNames
    //
    
    public static final String[] COLUMN_NAMES = {
        "col1",
        "col2",
        "col3",
        "col4"
    };
    
    //
    // primaryKeyColumnNames
    //
    
    public static final String[] PRIMARY_KEY_COLUMN_NAMES = {
    };
    
    //
    // javaNames
    //
    
    public static final String[] JAVA_NAMES = {
        "col1",
        "col2",
        "col3",
        "col4"
    };
    
    //
    // javaNamesProper
    //
    
    public static final String[] JAVA_NAMES_PROPER = {
        "Col1",
        "Col2",
        "Col3",
        "Col4"
    };
    
    
    //
    // member variables
    //
    
    private HashMap<String, String> descriptions = new HashMap<String, String>();
    
    private String col1;
    
    private Long col2;
    
    private String col3;
    
    private Long col4;
    
    //
    // trivial getters and setters
    //
    
    // col1
    
    public void setCol1(String val) {
        this.col1 = val;
    }
    
    public String getCol1() {
        return this.col1;
    }
    
    // col2
    
    public void setCol2(Long val) {
        this.col2 = val;
    }
    
    public Long getCol2() {
        return this.col2;
    }
    
    // col3
    
    public void setCol3(String val) {
        this.col3 = val;
    }
    
    public String getCol3() {
        return this.col3;
    }
    
    // col4
    
    public void setCol4(Long val) {
        this.col4 = val;
    }
    
    public Long getCol4() {
        return this.col4;
    }
    
    //
    // implementation of Dvo
    //
    
    public String getTableName() {
        return TABLE_NAME;
    };
    
    public String getSchemaName() {
        return SCHEMA_NAME;
    };
    
    public String[] getColumnNames() {
        return COLUMN_NAMES;
    };
    
    public String[] getPrimaryKeyColumnNames() {
        return PRIMARY_KEY_COLUMN_NAMES;
    };
    
    public String[] getJavaNames() {
        return JAVA_NAMES;
    };
    
    public String[] getJavaNamesProper() {
        return JAVA_NAMES_PROPER;
    };
    
    public void setDescriptions(HashMap<String, String> descriptions) {
        this.descriptions = descriptions;
    }
    
    public HashMap<String, String> getDescriptions() {
        return this.descriptions;
    }
    
    public void addDescription(String javaName, String value) {
        this.descriptions.put(javaName, value);
    }
    
    public String getDescription(String javaName) {
        return this.descriptions.get(javaName);
    }
    
    public String[] getPrimaryKeyValues() {
        String[] rtn = new String[] {
        };
        return rtn;
    }
}
