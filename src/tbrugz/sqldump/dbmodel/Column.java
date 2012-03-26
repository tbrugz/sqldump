package tbrugz.sqldump.dbmodel;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.Utils;

public class Column extends DBIdentifiable implements Serializable {
	
	public static class ColTypeUtil {
		static Properties dbmsSpecificProps;
		static List<String> doNotUsePrecision;
		
		static {
			dbmsSpecificProps = new ParametrizedProperties();
			try {
				InputStream is = ColTypeUtil.class.getClassLoader().getResourceAsStream(Defs.DBMS_SPECIFIC_RESOURCE);
				if(is==null) throw new IOException("resource "+Defs.DBMS_SPECIFIC_RESOURCE+" not found");
				dbmsSpecificProps.load(is);
				
				doNotUsePrecision = Utils.getStringListFromProp(dbmsSpecificProps, "type.donotuseprecision", ",");
			}
			catch(IOException e) {
				log.warn("Error loading typeMapping from resource: "+Defs.DBMS_SPECIFIC_RESOURCE);
				e.printStackTrace();
			}
		}
		
		public static boolean usePrecision(String colType) {
			colType = colType.toUpperCase();
			if(doNotUsePrecision.contains(colType)) { return false; }
			return !"false".equals(dbmsSpecificProps.getProperty("type."+colType+".useprecision"));
		}
	}
	
	private static final long serialVersionUID = 1L;
	static Log log = LogFactory.getLog(Column.class);
	
	public String type;
	public int columSize; //XXX: should be Integer? for columns that doesn't have precision could be null
	public Integer decimalDigits;
	transient boolean pk; //XXXdone: should be transient? add PK info into constraint? yes, yes
	public boolean nullable;
	String defaultValue;
	String remarks;
	
	public static String getColumnDesc(Column c) {
		return getColumnDesc(c, null, null);
	}
	
	/*public static String getColumnDesc(Column c, String fromDbId, String toDbId) {
		return getColumnDesc(c, fromDbId, toDbId);
	}*/
	
	//XXX: should be 'default'?
	public static String getColumnDesc(Column c, String fromDbId, String toDbId) {
		String colType = c.type.trim();
		
		/*if(fromDbId!=null && toDbId!=null) {
			if(fromDbId.equals(toDbId)) {
				//no conversion
			}
			else {
				colType = colType.toUpperCase();
				String ansiColType = ColTypeUtil.dbmsSpecificProps.getProperty("from."+fromDbId+"."+colType);
				String newColType = null;
				if(ansiColType!=null) {
					ansiColType = ansiColType.toUpperCase();
					newColType = ColTypeUtil.dbmsSpecificProps.getProperty("to."+toDbId+"."+ansiColType);
				}
				
				if(newColType!=null) {
					//log.debug("new col: "+newColType+"; old: "+colType);
					colType = newColType;
				}
				else if(ansiColType!=null) {
					//log.debug("ansi new col: "+ansiColType+"; old: "+colType);
					colType = ansiColType;
				}
				else {
					//log.debug("old col type: "+colType);
					colType = c.type.trim();
				}
			}
		}*/
		
		boolean usePrecision = ColTypeUtil.usePrecision(colType);

		return DBObject.getFinalIdentifier(c.name)+" "+colType
			+(usePrecision?"("+c.columSize+(c.decimalDigits!=null?","+c.decimalDigits:"")+")":"")
			+(c.defaultValue!=null?" default "+c.defaultValue:"")
			+(!c.nullable?" not null":"");
	}
	
	public static String getColumnDescFull(Column c, String fromDbId, String toDbId) {
		return getColumnDesc(c, fromDbId, toDbId)+(c.pk?" primary key":"");
	}

	//XXX: should be 'default'?
	//public static String getColumnDesc(Column c) {
		//return getColumnDesc(c, null, null);
		/*String colType = c.type;
		
		boolean usePrecision = true;
		if(typeMapping!=null) {
			usePrecision = !"false".equals(typeMapping.getProperty("type."+colType+".useprecision"));
		}
		
		return c.name+" "+colType
			+(usePrecision?"("+c.columSize+(c.decimalDigits!=null?","+c.decimalDigits:"")+")":"")
			+(!c.nullable?" not null":"");*/
	//}
	
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
