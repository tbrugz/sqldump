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
		public static final String PROP_IGNOREPRECISION = "sqldump.sqltypes.ignoreprecision";
		public static final String PROP_USEPRECISION = "sqldump.sqltypes.useprecision";
		
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
		
		public static void setProperties(Properties prop) {
			//ignoreprecision
			{
				List<String> sqlTypesIgnorePrecision = Utils.getStringListFromProp(prop, PROP_IGNOREPRECISION, ",");
				if(sqlTypesIgnorePrecision!=null) {
					doNotUsePrecision.addAll(sqlTypesIgnorePrecision);
				}
			}
			
			//useprecision
			{
				List<String> sqlTypesUsePrecision = Utils.getStringListFromProp(prop, PROP_USEPRECISION, ",");
				if(sqlTypesUsePrecision!=null) {
					doNotUsePrecision.removeAll(sqlTypesUsePrecision);
					for(String ctype: sqlTypesUsePrecision) {
						dbmsSpecificProps.remove("type."+ctype+".useprecision");
					}
				}
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
	
	//XXX: should be 'default'?
	public static String getColumnDesc(Column c) {
		String colType = c.type.trim();
		
		boolean usePrecision = ColTypeUtil.usePrecision(colType);

		return DBObject.getFinalIdentifier(c.name)+" "+colType
			+(usePrecision?"("+c.columSize+(c.decimalDigits!=null?","+c.decimalDigits:"")+")":"")
			+(c.defaultValue!=null?" default "+c.defaultValue:"")
			+(!c.nullable?" not null":"");
	}
	
	public static String getColumnDescFull(Column c) {
		return getColumnDesc(c)+(c.pk?" primary key":"");
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
