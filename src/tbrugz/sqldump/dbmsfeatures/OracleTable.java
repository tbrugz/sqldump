package tbrugz.sqldump.dbmsfeatures;

import tbrugz.sqldump.dbmodel.Table;

//TODO: property for selecting dump (true/false) of extra fields on script output?
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
}
