package tbrugz.sqldump.dbmsfeatures;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.AbstractDatabaseMetaDataDecorator;
import tbrugz.sqldump.util.QueryWithParams;

public class InformationSchemaDatabaseMetaData extends AbstractDatabaseMetaDataDecorator {

	static final Log log = LogFactory.getLog(InformationSchemaDatabaseMetaData.class);
	
	public InformationSchemaDatabaseMetaData(DatabaseMetaData metadata) {
		super(metadata);
	}
	
	QueryWithParams getTablesQuery(String schemaPattern, String tableNamePattern) {
		String sql = "select table_catalog as table_cat, table_schema as table_schem, table_name,\n"+
				"table_type, null as remarks,\n"+
				"user_defined_type_catalog as type_cat, user_defined_type_schema as type_schem, user_defined_type_name as type_name,\n"+
				"self_referencing_column_name, reference_generation as ref_generation\n"+
				"from information_schema.tables t\n"+
				"where 1=1\n";
		List<Object> params = new ArrayList<>();
		if(schemaPattern!=null) {
			sql += "and table_schema like ?\n";
			params.add(schemaPattern);
		}
		if(tableNamePattern!=null) {
			sql += "and table_name like ?\n";
			params.add(tableNamePattern);
		}
		return new QueryWithParams(sql, params);
	}
	
	ResultSet getTablesInternal(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			throws SQLException {
		QueryWithParams qwp = getTablesQuery(schemaPattern, tableNamePattern);
		log.debug("getTablesInternal: sql: "+qwp);
		PreparedStatement st = getConnection().prepareStatement(qwp.getQuery());
		qwp.setParameters(st);
		return st.executeQuery();
	}

}
