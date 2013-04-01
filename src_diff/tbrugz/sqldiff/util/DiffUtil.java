package tbrugz.sqldiff.util;

import java.util.Collection;

import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;

public class DiffUtil {
	
	//XXX rename to getDumpeableByXXX?
	@SuppressWarnings("unchecked")
	public static <T extends DBIdentifiable> T getDBIdentifiableByTypeSchemaAndName(Collection<? extends DBIdentifiable> dbids, DBObjectType type, String schemaName, String name) {
		for(DBIdentifiable d: dbids) {
			if(type.equals(DBIdentifiable.getType4Diff(d)) 
					&& (d.getSchemaName()!=null?d.getSchemaName().equalsIgnoreCase(schemaName):true) 
					&& d.getName().equalsIgnoreCase(name)
					&& d.isDumpable())
				return (T) d;
		}
		return null;
	}

}
