package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.DBMSUpdateListener;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.Utils;

public class Column extends DBIdentifiable implements Serializable, Cloneable {
	
	//XXXxx: refactoring: move to its own class & another package?
	public static class ColTypeUtil {
		public static final String PROP_IGNOREPRECISION = "sqldump.sqltypes.ignoreprecision";
		public static final String PROP_USEPRECISION = "sqldump.sqltypes.useprecision";
		public static final String PROP_DONOTUSEPRECISION = "type.donotuseprecision";
		
		static final Properties dbmsSpecificProps = new ParametrizedProperties();
		static final Map<String, Boolean> usePrecisionMap = new HashMap<String, Boolean>();
		
		final static DBMSUpdateListener updateListener = new DBMSUpdateListener() {
			@Override
			public void dbmsUpdated() {
				init();
			}
		};
		
		static {
			init();
			DBMSResources.instance().addUpdateListener(updateListener);
		}
		
		static void init() {
				dbmsSpecificProps.clear();
				dbmsSpecificProps.putAll(DBMSResources.instance().getProperties());
				
				synchronized (usePrecisionMap) {
					usePrecisionMap.clear();
					List<String> doNotUsePrecisionTypeList = Utils.getStringListFromProp(dbmsSpecificProps, PROP_DONOTUSEPRECISION, ",");
					if(doNotUsePrecisionTypeList!=null) {
						for(String type: doNotUsePrecisionTypeList) {
							usePrecisionMap.put(type.toUpperCase(), false);
						}
					}
				}
		}
		
		public static void setProperties(Properties prop) {
			init();
			if(prop==null) {
				//reset...
				return;
			}
			
			//ignoreprecision
			{
				List<String> sqlTypesIgnorePrecision = Utils.getStringListFromProp(prop, PROP_IGNOREPRECISION, ",");
				if(sqlTypesIgnorePrecision!=null) {
					for(String s: sqlTypesIgnorePrecision) {
						usePrecisionMap.put(s.toUpperCase(), false);
					}
				}
			}
			
			//useprecision
			{
				List<String> sqlTypesUsePrecision = Utils.getStringListFromProp(prop, PROP_USEPRECISION, ",");
				if(sqlTypesUsePrecision!=null) {
					for(String ctype: sqlTypesUsePrecision) {
						usePrecisionMap.remove(ctype.toUpperCase());
						dbmsSpecificProps.remove("type."+ctype.toUpperCase()+".useprecision");
					}
				}
			}
		}
		
		public static boolean usePrecision(String colType) {
			if(colType==null) { throw new IllegalArgumentException("colType should not be null"); }
			colType = colType.toUpperCase();
			if(usePrecisionMap.containsKey(colType)) { return usePrecisionMap.get(colType); }
			String usePrecision = dbmsSpecificProps.getProperty("type."+colType+".useprecision");
			if(usePrecision==null) {
				usePrecision = dbmsSpecificProps.getProperty("type."+colType+"@"+DBMSResources.instance().dbid()+".useprecision");
			}
			return !"false".equals(usePrecision);
		}
	}
	
	private static final long serialVersionUID = 1L;
	static Log log = LogFactory.getLog(Column.class);
	
	public String type;
	public Integer columSize;
	public Integer decimalDigits;
	transient boolean pk;
	public boolean nullable = true;
	String defaultValue;
	String remarks;
	public Boolean autoIncrement;
	//XXX add transient String tableName; //??
	//XXX add column position in table? nice for column compare...

	public static transient boolean useAutoIncrement = false;
	
	public static String getColumnDescFull(Column c) {
		return c.getDefinition()+(c.pk?" primary key":"");
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Column) {
			Column c = (Column) obj;
			return name.equals(c.name) && type.equals(c.type)
					&& (columSize!=null?columSize.equals(c.columSize):c.columSize==null)
					&& (decimalDigits!=null?decimalDigits.equals(c.decimalDigits):c.decimalDigits==null)
					&& (defaultValue!=null?defaultValue.equals(c.defaultValue):c.defaultValue==null)
					&& (nullable == c.nullable)
					&& (pk==c.pk);
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
		return "[column:"+this.getDefinition()+"]";
		//return "[column: "+name+" "+type+"]";
	}

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return getDefinition();
	}

	public String getDefinition() {
		return DBObject.getFinalIdentifier(name)+" "+getTypeDefinition();
	}
	
	public String getTypeDefinition() {
		String colType = type.trim();
		boolean usePrecision = ColTypeUtil.usePrecision(colType);
		return colType
			+(usePrecision?"("+columSize+(decimalDigits!=null?","+decimalDigits:"")+")":"")
			+(defaultValue!=null?" default "+defaultValue:"")
			+(!nullable?" not null":"")
			+(useAutoIncrement?
				((autoIncrement!=null && autoIncrement)?" auto_increment":"")
			 :"");
	}
	
	@Override
	public Column clone() {
		try {
			return (Column) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
	
}
