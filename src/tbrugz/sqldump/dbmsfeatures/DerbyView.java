package tbrugz.sqldump.dbmsfeatures;

import tbrugz.sqldump.dbmodel.View;

public class DerbyView extends View {

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return query+";";
	}

}
