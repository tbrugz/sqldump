package tbrugz.sqldump.dbmodel;

import java.io.Serializable;
import java.util.Properties;

public class Column implements Serializable {
	public String name;
	public String type;
	public int columSize;
	public Integer decimalDigits;
	public boolean pk;
	public boolean nullable;
	
	public static String getColumnDesc(Column c, Properties typeMapping, String fromDbId, String toDbId) {
		String colType = c.type;
		
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
			usePrecision = !"false".equals(typeMapping.getProperty("type."+colType+".useprecision"));
		}
		return c.name+" "+colType
			+(usePrecision?"("+c.columSize+(c.decimalDigits!=null?","+c.decimalDigits:"")+")":"")
			+(!c.nullable?" not null":"");
	}
	
	public static String getColumnDescFull(Column c, Properties typeMapping, String fromDbId, String toDbId) {
		return getColumnDesc(c, typeMapping, fromDbId, toDbId)+(c.pk?" primary key":"");
	}

	//??
	public static String getColumnDesc(Column c, Properties typeMapping) {
		String colType = c.type;
		
		boolean usePrecision = true;
		if(typeMapping!=null) {
			usePrecision = !"false".equals(typeMapping.getProperty("type."+colType+".useprecision"));
		}
		
		return c.name+" "+colType
			+(usePrecision?"("+c.columSize+(c.decimalDigits!=null?","+c.decimalDigits:"")+")":"")
			+(!c.nullable?" not null":"");
	}
	
}
