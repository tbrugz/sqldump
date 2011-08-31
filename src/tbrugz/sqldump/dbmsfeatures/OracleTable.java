package tbrugz.sqldump.dbmsfeatures;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Table;

/*
 * TODO: property for selecting dump (true/false) of extra fields on script output?
 * XXXxxx: sys.all_external_tables, sys.all_mviews
 */
public class OracleTable extends Table {
	public String tableSpace;
	public boolean temporary;
	public boolean logging;
	
	@Override
	public String getTableType4sql() {
		return temporary?"global temporary ":"";
	}
	
	@Override
	public String getTableFooter4sql() {
		String footer = tableSpace!=null?"\nTABLESPACE "+tableSpace:"";
		footer += logging?"\nLOGGING":"";
		return footer; 
	}
	
	@Override
	public String getAfterCreateTableScript() {
		//COMMENT ON COLUMN [schema.]table.column IS 'text'
		StringBuffer sb = new StringBuffer();
		for(Column c: getColumns()) {
			String comment = c.getComment();
			if(comment!=null && !comment.trim().equals("")) {
				//XXX: escape comment?
				sb.append("comment on column "+schemaName+"."+name+"."+c.name+" is '"+comment+"';\n");
			}
		}
		return sb.toString();
	}
}
