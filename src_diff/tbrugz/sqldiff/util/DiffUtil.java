package tbrugz.sqldiff.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldiff.datadiff.DiffSyntax;
import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.util.Utils;

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
	
	public static List<DiffSyntax> getSyntaxes(Properties prop, DBMSFeatures feat, String propKey) {
		List<DiffSyntax> dss = new ArrayList<DiffSyntax>();
		
		List<String> syntaxes = Utils.getStringListFromProp(prop, propKey, ",");
		if(syntaxes!=null) {
			for(String s: syntaxes) {
				DiffSyntax ds = getSyntax(prop, feat, s);
				if(ds!=null) {
					dss.add(ds);
				}
			}
		}
		
		return dss;
	}

	public static DiffSyntax getSyntax(Properties prop, DBMSFeatures feat, String className) {
		DiffSyntax ds = (DiffSyntax) Utils.getClassInstance(className, "tbrugz.sqldiff.datadiff");
		if(ds!=null) {
			ds.procProperties(prop);
			if(ds.needsDBMSFeatures()) { ds.setFeatures(feat); }
			return ds;
		}
		return null;
	}

}
