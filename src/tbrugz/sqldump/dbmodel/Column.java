package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.Utils;

public class Column extends DBIdentifiable implements Serializable, Cloneable, RemarkableDBObject {
	
	//XXXxx: refactoring: move to its own class & another package? no, Column is the only 'user' of this
	public static class ColTypeUtil {
		public static final String PROP_IGNOREPRECISION = "sqldump.sqltypes.ignoreprecision";
		public static final String PROP_USEPRECISION = "sqldump.sqltypes.useprecision";
		public static final String DBPROP_COLUMN_USEAUTOINCREMENT = "column.useautoincrement";
		
		// variables based only on "dbms-specific.properties"
		static final Properties dbmsSpecificProps = new ParametrizedProperties();
		static final List<String> useAutoIncrement = new ArrayList<String>();
		
		// variables also based on app properties 
		static final Map<String, Boolean> usePrecisionMap = new HashMap<String, Boolean>();
		
		static String dbid;
		
		public static void init(Properties prop) { //DBMS
			usePrecisionMap.clear();
			if(prop!=null) {
					dbmsSpecificProps.clear();
					dbmsSpecificProps.putAll(prop);
					//setupPrecisionMap(prop);
					setupStaticVars(prop);
			}
		}
		
		public static void setProperties(Properties prop) {
			if(prop==null) {
				//reset...
				usePrecisionMap.clear();
			}
			else {
				setupPrecisionMap(prop);
			}
		}
		
		static void setupPrecisionMap(Properties prop) {
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
						usePrecisionMap.put(ctype.toUpperCase(), true);
					}
				}
			}
		}
			
		static void setupStaticVars(Properties prop) {
			//auto_increment
			List<String> autoIncIds = Utils.getStringListFromProp(dbmsSpecificProps, DBPROP_COLUMN_USEAUTOINCREMENT, ",");
			if(autoIncIds!=null) {
				useAutoIncrement.addAll( autoIncIds );
			}
		}

		public static void setDbId(String dbidParam) {
			dbid = dbidParam;
			usePrecisionMap.clear();
			DBMSResources.instance().updateSpecificFeaturesClass(dbid);
		}
		
		public static boolean usePrecision(String colType) {
			if(colType==null) { throw new IllegalArgumentException("colType should not be null"); }
			colType = colType.toUpperCase();
			if(usePrecisionMap.containsKey(colType)) { return usePrecisionMap.get(colType); }
			Boolean usePrecision = Utils.getPropBoolean(dbmsSpecificProps, "type."+colType+".useprecision");
			if(usePrecision==null) {
				usePrecision = Utils.getPropBoolean(dbmsSpecificProps, "type."+colType+"@"+dbid+".useprecision");
			}
			// add types.useprecision.regex? types.ignoreprecision.regex? ex: types.ignoreprecision.regex=TIMESTAMP\(\d+\)
			if(usePrecision==null) {
				if(colType.contains("(")) { usePrecision = false; }
			}
			return usePrecision==null || usePrecision;
		}

		public static boolean useAutoIncrement() {
			return useAutoIncrement.contains(dbid);
		}
		
		static String upper(String s) {
			if(s==null) return null;
			return s.toUpperCase();
		}
		
		public static boolean isInteger(String type) {
			return Utils.getStringListFromProp(dbmsSpecificProps, "types.integer", ",").contains(upper(type));
		}

		public static boolean isNumeric(String type) {
			return Utils.getStringListFromProp(dbmsSpecificProps, "types.numeric", ",").contains(upper(type));
		}

		public static boolean isCharacter(String type) {
			return Utils.getStringListFromProp(dbmsSpecificProps, "types.character", ",").contains(upper(type));
		}
		
		public static boolean isDatetime(String type) {
			return Utils.getStringListFromProp(dbmsSpecificProps, "types.datetime", ",").contains(upper(type));
		}
		
		public static boolean isBinary(String type) {
			return Utils.getStringListFromProp(dbmsSpecificProps, "types.binary", ",").contains(upper(type));
		}
		
		public static boolean isBoolean(String type) {
			return Utils.getStringListFromProp(dbmsSpecificProps, "types.boolean", ",").contains(upper(type));
		}
		
	}
	
	private static final long serialVersionUID = 1L;
	static final Log log = LogFactory.getLog(Column.class);
	
	String type;
	Integer columnSize;
	Integer decimalDigits;
	transient boolean pk;
	boolean nullable = true;
	String defaultValue;
	String remarks;
	Boolean autoIncrement; // GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY ; start-with, increment - see: https://blog.jooq.org/2012/02/19/subtle-sql-differences-identity-columns/ 
	//XXX add transient String tableName; //??
	//XXX Boolean updateable; //?? - http://english.stackexchange.com/questions/56431/correct-spelling-updatable-or-updateable
	int ordinalPosition; //XXXdone add column position in table? nice for column compare...

	//public static transient boolean useAutoIncrement = false;
	
	public static String getColumnDescFull(Column c) {
		return c.getDefinition()+(c.pk?" primary key":"");
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Column) {
			Column c = (Column) obj;
			return name.equals(c.name)
					&& (type!=null?type.equals(c.type):c.type==null)
					&& (columnSize!=null?columnSize.equals(c.columnSize):c.columnSize==null)
					&& (decimalDigits!=null?decimalDigits.equals(c.decimalDigits):c.decimalDigits==null)
					&& (defaultValue!=null?defaultValue.equals(c.defaultValue):c.defaultValue==null)
					&& (nullable == c.nullable)
					&& (pk==c.pk);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + ((columnSize == null) ? 0 : columnSize.hashCode());
		result = prime * result + ((decimalDigits == null) ? 0 : decimalDigits.hashCode());
		result = prime * result + ((defaultValue == null) ? 0 : defaultValue.hashCode());
		result = prime * result + ((nullable) ? 0 : 1);
		result = prime * result + ((pk) ? 0 : 2);
		return result;
	}
	
	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
	
	@Override
	public String getRemarks() {
		return remarks;
	}

	@Override
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
		return DBObject.getFinalIdentifier(name)+" "+getTypeDefinition()+getColumnConstraints();
	}
	
	public String getDefinition4Diff() {
		return DBObject.getFinalIdentifier(name)+" "+getTypeDefinition(true)+getColumnConstraints();
	}
	
	public String getTypeDefinition() {
		return getTypeDefinition(false);
	}
	
	String getTypeDefinition(boolean upper) {
		if(type==null) { return null; }
		String colType = upper?type.trim().toUpperCase():type.trim();
		boolean usePrecision = ColTypeUtil.usePrecision(colType);
		return colType
			+((usePrecision && columnSize!=null && columnSize>0)?"("+columnSize+(decimalDigits!=null?","+decimalDigits:"")+")":"");
	}
	
	public String getColumnConstraints() {
		/*if((autoIncrement!=null && autoIncrement)) {
			log.info("auto-increment["+name+"]: useAutoIncrement? "+ColTypeUtil.useAutoIncrement()+" [dbid="+ColTypeUtil.dbid+"]");
		}*/
		return getDefaultSnippet()
			+getNullableSnippet()
			+(ColTypeUtil.useAutoIncrement()?
				((autoIncrement!=null && autoIncrement)?" auto_increment":"")
			:"");
	}

	public String getFullColumnConstraints() {
		return getDefaultSnippet()
			+getFullNullableSnippet()
			+(ColTypeUtil.useAutoIncrement()?
				((autoIncrement!=null && autoIncrement)?" auto_increment":"")
			:"");
	}
	
	//XXX: complete syntax parameter? may return 'default null'
	public String getDefaultSnippet() {
		return (defaultValue!=null?" default "+defaultValue:"");
	}

	public String getFullDefaultSnippet() {
		return (defaultValue!=null?" default "+defaultValue:" default null");
	}
	
	//XXX: complete syntax parameter? may return 'null'
	public String getNullableSnippet() {
		return (!nullable?" not null":"");
	}

	public String getFullNullableSnippet() {
		return (!nullable?" not null":" null");
	}
	
	@Override
	public Column clone() {
		try {
			return (Column) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Integer getColumnSize() {
		return columnSize;
	}

	public void setColumnSize(Integer columnSize) {
		this.columnSize = columnSize;
	}

	public Integer getDecimalDigits() {
		return decimalDigits;
	}

	public void setDecimalDigits(Integer decimalDigits) {
		this.decimalDigits = decimalDigits;
	}

	public boolean isPk() {
		return pk;
	}

	public void setPk(boolean pk) {
		this.pk = pk;
	}

	public boolean isNullable() {
		return nullable;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	public Boolean getAutoIncrement() {
		return autoIncrement;
	}

	public void setAutoIncrement(Boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
	}

	public int getOrdinalPosition() {
		return ordinalPosition;
	}

	public void setOrdinalPosition(int ordinalPosition) {
		this.ordinalPosition = ordinalPosition;
	}
	
}
