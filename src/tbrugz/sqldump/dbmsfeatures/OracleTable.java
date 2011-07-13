package tbrugz.sqldump.dbmsfeatures;

import tbrugz.sqldump.dbmodel.Table;

public class OracleTable extends Table {
	String tableSpace;
	boolean temporary;
	boolean logging;
}
