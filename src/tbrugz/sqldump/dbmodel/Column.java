package tbrugz.sqldump.dbmodel;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.apache.log4j.Logger;

import tbrugz.sqldump.SQLDump;

public class Column extends DBIdentifiable implements Serializable {
	private static final long serialVersionUID = 1L;
	static Logger log = Logger.getLogger(Column.class);
	
	//public String name;
	public String type;
	public int columSize;
	public Integer decimalDigits;
	transient boolean pk; //XXXdone: should be transient? add PK info into constraint? yes, yes
	public boolean nullable;
	String defaultValue;
	String remarks;
	
	static Properties typeMapping = null;
	{
		typeMapping = new Properties();
		try {
			typeMapping.load(SQLDump.class.getClassLoader().getResourceAsStream(SQLDump.DBMS_SPECIFIC_RESOURCE));
		} catch (IOException e) {
			log.warn("Error loading typeMapping from resource: "+SQLDump.DBMS_SPECIFIC_RESOURCE);
			e.printStackTrace();
		}
	}
	
	public static String getColumnDesc(Column c) {
		return getColumnDesc(c, typeMapping, null, null);
	}
	
	public static String getColumnDesc(Column c, String fromDbId, String toDbId) {
		return getColumnDesc(c, typeMapping, fromDbId, toDbId);
	}
	
	//XXX: should be 'default'?
	public static String getColumnDesc(Column c, Properties typeMapping, String fromDbId, String toDbId) {
		String colType = c.type.trim();
		
		if(fromDbId!=null && toDbId!=null) {
			if(fromDbId.equals(toDbId)) {
				//no conversion
			}
			else {
				String ansiColType = typeMapping.getProperty("from."+fromDbId+"."+colType);
				if(ansiColType!=null) { colType = ansiColType; }
				String newColType = typeMapping.getProperty("to."+toDbId+"."+colType);
				if(newColType!=null) { colType = newColType; }
			}
		}
		
		boolean usePrecision = true;
		if(typeMapping!=null) {
			usePrecision = !"false".equals(typeMapping.getProperty("type."+colType.toUpperCase()+".useprecision"));
		}
		return c.name+" "+colType
			+(usePrecision?"("+c.columSize+(c.decimalDigits!=null?","+c.decimalDigits:"")+")":"")
			+(c.defaultValue!=null?" default "+c.defaultValue:"")
			+(!c.nullable?" not null":"");
	}
	
	public static String getColumnDescFull(Column c, Properties typeMapping, String fromDbId, String toDbId) {
		return getColumnDesc(c, typeMapping, fromDbId, toDbId)+(c.pk?" primary key":"");
	}

	//XXX: should be 'default'?
	public static String getColumnDesc(Column c, Properties typeMapping) {
		return getColumnDesc(c, typeMapping, null, null);
		/*String colType = c.type;
		
		boolean usePrecision = true;
		if(typeMapping!=null) {
			usePrecision = !"false".equals(typeMapping.getProperty("type."+colType+".useprecision"));
		}
		
		return c.name+" "+colType
			+(usePrecision?"("+c.columSize+(c.decimalDigits!=null?","+c.decimalDigits:"")+")":"")
			+(!c.nullable?" not null":"");*/
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Column) {
			Column c = (Column) obj;
			return name.equals(c.name) && type.equals(c.type) && (columSize==c.columSize) 
					&& (decimalDigits!=null?decimalDigits.equals(c.decimalDigits):c.decimalDigits==null) 
					&& (defaultValue!=null?defaultValue.equals(c.defaultValue):c.defaultValue==null) 
					&& (pk==c.pk) && (nullable==c.nullable);
		}
		return false;
	}
	
	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
	
	public String getRemarks() {
		return remarks;
	}

	public void setRemarks(String comment) {
		this.remarks = comment;
	}

	@Override
	public String toString() {
		return "[column:"+getColumnDesc(this)+"]";
		//return "[column: "+name+" "+type+"]";
	}

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return getColumnDesc(this);
	}
	
}
