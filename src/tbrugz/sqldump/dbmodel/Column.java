package tbrugz.sqldump.dbmodel;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.Utils;

public class Column extends DBIdentifiable implements Serializable, Cloneable {
	
	//XXX: refactoring: move to another package?
	public static class ColTypeUtil {
		public static final String PROP_IGNOREPRECISION = "sqldump.sqltypes.ignoreprecision";
		public static final String PROP_USEPRECISION = "sqldump.sqltypes.useprecision";
		public static final String PROP_DONOTUSEPRECISION = "type.donotuseprecision";
		
		static final Properties dbmsSpecificProps = new ParametrizedProperties();
		static final List<String> doNotUsePrecision = new Vector<String>();
		
		static {
			init();
		}
		
		static void init() {
			try {
				InputStream is = ColTypeUtil.class.getClassLoader().getResourceAsStream(Defs.DBMS_SPECIFIC_RESOURCE);
				if(is==null) throw new IOException("resource "+Defs.DBMS_SPECIFIC_RESOURCE+" not found");
				dbmsSpecificProps.load(is);
				
				doNotUsePrecision.clear();
				List<String> doNotUsePrecisionTypeList = Utils.getStringListFromProp(dbmsSpecificProps, PROP_DONOTUSEPRECISION, ",");
				if(doNotUsePrecisionTypeList!=null) {
					doNotUsePrecision.addAll(doNotUsePrecisionTypeList);
				}
			}
			catch(Exception e) {
				log.warn("Error loading typeMapping from resource: "+Defs.DBMS_SPECIFIC_RESOURCE);
				e.printStackTrace();
			}
		}
		
		public static void setProperties(Properties prop) {
			if(prop==null) {
				//reset...
				init();
				return;
			}
			
			//ignoreprecision
			{
				List<String> sqlTypesIgnorePrecision = Utils.getStringListFromProp(prop, PROP_IGNOREPRECISION, ",");
				if(sqlTypesIgnorePrecision!=null) {
					for(String s: sqlTypesIgnorePrecision) {
						doNotUsePrecision.add(s.toUpperCase());
					}
				}
			}
			
			//useprecision
			{
				List<String> sqlTypesUsePrecision = Utils.getStringListFromProp(prop, PROP_USEPRECISION, ",");
				if(sqlTypesUsePrecision!=null) {
					for(String ctype: sqlTypesUsePrecision) {
						doNotUsePrecision.remove(ctype.toUpperCase());
						dbmsSpecificProps.remove("type."+ctype.toUpperCase()+".useprecision");
					}
				}
			}
		}
		
		public static boolean usePrecision(String colType) {
			if(colType==null) { return false; }
			colType = colType.toUpperCase();
			if(doNotUsePrecision.contains(colType)) { return false; }
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
	public Integer columSize; //XXXdone: should be Integer? for columns that doesn't have precision could be null. yes!
	public Integer decimalDigits;
	transient boolean pk; //XXXdone: should be transient? add PK info into constraint? yes, yes
	public Boolean nullable;
	String defaultValue;
	String remarks;
	public Boolean autoIncrement;

	public static transient boolean useAutoIncrement = false;
	
	//XXX: should be 'default'?
	public static String getColumnDesc(Column c) {
		String colType = c.type!=null ? c.type.trim() : null;
		
		boolean usePrecision = ColTypeUtil.usePrecision(colType);

		return DBObject.getFinalIdentifier(c.name)+" "+colType
			+(usePrecision?"("+c.columSize+(c.decimalDigits!=null?","+c.decimalDigits:"")+")":"")
			+(c.defaultValue!=null?" default "+c.defaultValue:"")
			+(c.nullable!=null && !c.nullable?" not null":"")
			+(useAutoIncrement?
				((c.autoIncrement!=null && c.autoIncrement)?" auto_increment":"")
			 :"");
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
	
	@Override
	public Column clone() {
		try {
			return (Column) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
	
}
