package tbrugz.sqldump.dbmsfeatures;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.NamedDBObject;

public abstract class PostgreSQLAbstractFeatutres extends InformationSchemaFeatures {

	@Override
	public boolean supportsDiffingColumn() {
		return true;
	}

	@Override
	public String sqlAlterColumnByDiffing(NamedDBObject table, Column previousColumn,
			Column column) {
		//diff type, if no diff: diff default, if no diff: diff nullable
		//see: http://www.postgresql.org/docs/9.1/static/sql-altertable.html
		if(!previousColumn.getTypeDefinition().equals(column.getTypeDefinition())) {
			return createAlterColumn(table, column,
					" set data type "+column.getTypeDefinition());
		}
		else if(!previousColumn.getDefaultSnippet().equals(column.getDefaultSnippet())) {
			return createAlterColumn(table, column,
					(column.getDefaultSnippet().trim().equals("")?" drop default":" set"+column.getDefaultSnippet()));
		}
		else if(!previousColumn.getNullableSnippet().equals(column.getNullableSnippet())) {
			return createAlterColumn(table, column,
					(column.isNullable()?" drop":" set")+" not null");
		}
		
		throw new UnsupportedOperationException("no differences between PostgreSQL columns found");
	}
	
}
