package tbrugz.sqldiff.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;

public class DiffUtil {
	
	//XXX rename to getDumpableByXXX?
	@SuppressWarnings("unchecked")
	public static <T extends DBIdentifiable> T getDBIdentifiableByTypeSchemaAndName(Collection<? extends DBIdentifiable> dbids, DBObjectType type, String schemaName, String name) {
		for(DBIdentifiable d: dbids) {
			if(type.equals(DBIdentifiable.getType(d)) 
					&& (d.getSchemaName()!=null?d.getSchemaName().equalsIgnoreCase(schemaName):true) 
					&& d.getName().equalsIgnoreCase(name)
					&& d.isDumpable())
				return (T) d;
		}
		return null;
	}
	
	public static <T extends Object> List<T> singleElemList(T s) {
		List<T> ret = new ArrayList<T>();
		ret.add(s);
		return ret;
	}

}
