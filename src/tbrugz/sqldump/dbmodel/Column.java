package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.Utils;

public class Column extends DBIdentifiable implements Serializable, Cloneable {
	
	//XXXxx: refactoring: move to its own class & another package? no, Column is the only 'user' of this
	public static class ColTypeUtil {
		public static final String PROP_IGNOREPRECISION = "sqldump.sqltypes.ignoreprecision";
		public static final String PROP_USEPRECISION = "sqldump.sqltypes.useprecision";
		
		static final Properties dbmsSpecificProps = new ParametrizedProperties();
		static final Map<String, Boolean> usePrecisionMap = new HashMap<String, Boolean>();
		
		static String dbid;
		
		public static void init(Properties prop) { //DBMS
			usePrecisionMap.clear();
			if(prop!=null) {
					dbmsSpecificProps.clear();
					dbmsSpecificProps.putAll(prop);
					setupPrecisionMap(prop);
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

		public static void setDbId(String dbidParam) {
			dbid = dbidParam;
			usePrecisionMap.clear();
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
	}
	
	private static final long serialVersionUID = 1L;
	static Log log = LogFactory.getLog(Column.class);
	
	String type;
	Integer columnSize;
	Integer decimalDigits;
	transient boolean pk;
	boolean nullable = true;
	String defaultValue;
	String remarks;
	Boolean autoIncrement;
	//XXX add transient String tableName; //??
	//XXX Boolean updateable; //?? - http://english.stackexchange.com/questions/56431/correct-spelling-updatable-or-updateable
	int ordinalPosition; //XXXdone add column position in table? nice for column compare...

	public static transient boolean useAutoIncrement = false;
	
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
		return DBObject.getFinalIdentifier(name)+" "+getTypeDefinition()+getColumnConstraints();
	}
	
	public String getTypeDefinition() {
		if(type==null) { return null; }
		String colType = type.trim();
		boolean usePrecision = ColTypeUtil.usePrecision(colType);
		return colType
			+((usePrecision && columnSize!=null)?"("+columnSize+(decimalDigits!=null?","+decimalDigits:"")+")":"");
	}
	
	public String getColumnConstraints() {
		return getDefaultSnippet()
			+getNullableSnippet()
			+(useAutoIncrement?
				((autoIncrement!=null && autoIncrement)?" auto_increment":"")
			:"");
	}
	
	//XXX: complete syntax parameter? may return 'default null'
	public String getDefaultSnippet() {
		return (defaultValue!=null?" default "+defaultValue:"");
	}
	
	//XXX: complete syntax parameter? may return 'null'
	public String getNullableSnippet() {
		return (!nullable?" not null":"");
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

	//TODO: rename [g|s]etColumSize -> [g|s]etColumnSize
	public Integer getColumSize() {
		return columnSize;
	}

	public void setColumSize(Integer columnSize) {
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
