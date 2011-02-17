package tbrugz.sqldump.dbmodel;

import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.Utils;

/*
 * see: http://download.oracle.com/docs/cd/B19306_01/server.102/b14200/statements_5010.htm
 */
public class Index extends DBObject {
	
	public boolean unique;
	public String tableName;
	public List<String> columns = new ArrayList<String>();
	
    @Override
    public String getDefinition(boolean dumpSchemaName) {
    	return "create "+(unique?"unique ":"")+"index "+(dumpSchemaName?schemaName+".":"")+name+" on "+(dumpSchemaName?schemaName+".":"")+tableName
    		+" ("+Utils.join(columns, ", ")+");";
    }

}
