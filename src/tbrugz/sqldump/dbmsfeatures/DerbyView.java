package tbrugz.sqldump.dbmsfeatures;

import tbrugz.sqldump.dbmodel.View;

@Deprecated
public class DerbyView extends View {
	private static final long serialVersionUID = 1L;

	@Override
	public String getDefinition(boolean dumpSchemaName) {
		return query+";";
	}

}
