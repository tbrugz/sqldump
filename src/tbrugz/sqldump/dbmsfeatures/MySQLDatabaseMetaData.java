package tbrugz.sqldump.dbmsfeatures;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import tbrugz.sqldump.def.AbstractDatabaseMetaDataDecorator;

/*
 * XXX: change [catalog -> schema] in getColumns(), .getTablePrivileges(), .getPrimaryKeys(), getIndexInfo(), getImportedKeys(), getExportedKeys(),
 *   getProcedures(), getFunctions(), .getProcedureColumns(), .getFunctionColumns()
 *   see: https://kb.askmonty.org/en/mariadb-java-client-110-release-notes/ - CONJ-10, CONJ-8, CONJ-9 
 */
public class MySQLDatabaseMetaData extends AbstractDatabaseMetaDataDecorator {

	public MySQLDatabaseMetaData(DatabaseMetaData metadata) {
		super(metadata);
	}
	
	@Override
	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException {
		//changes catalog -> schema
		return super.getTables(schemaPattern, null, tableNamePattern, types);
	}

}
