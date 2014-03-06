package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.AbstractDatabaseMetaDataDecorator;
import tbrugz.sqldump.resultset.EmptyResultSet;

//TODO: getCrossReference()?
public class MSAccessDatabaseMetaData extends AbstractDatabaseMetaDataDecorator {

	static Log log = LogFactory.getLog(MSAccessDatabaseMetaData.class);

	public MSAccessDatabaseMetaData(DatabaseMetaData metadata) {
		super(metadata);
	}
	
	@Override
	public ResultSet getSchemas() throws SQLException {
		return null;
	}
	
	/*
	
	MSysObjects: type:
	1 - Table / System Table
	3? - system tables?
	5 - Query (View...)
	8 - Relationship
	-32768 - Form
	-32766 - Macro
	-32764 - Report
	-32761 - Module
	-32756 - Pages
	
	MSysQueries?
	
	MSysRelationships...
	
	*/
	
	/*
	@Override
	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException {
		Connection conn = metadata.getConnection();
		
		String sql = "select null as TABLE_CAT, null as TABLE_SCHEM, name as TABLE_NAME, '' as REMARKS, "
				+"iif([Type] = 5, 'VIEW', iif([Flags] = 0, 'TABLE', 'SYSTEM TABLE')) as TABLE_TYPE\n"
				//+"iif([Flags] = 0, 'TABLE', 'SYSTEM TABLE') as TABLE_TYPE\n"
				+"from [MSysObjects] "
				+"where [Type] in ( 1 , 5 ) "
				+"order by [Type], [Name]";
		PreparedStatement st = conn.prepareStatement(sql);
		log.debug("sql:\n"+sql);
		return st.executeQuery();
	}
	*/
	
	@Override
	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException {
		return super.getTables(null, null, tableNamePattern, types);
	}
	
	@Override
	public ResultSet getColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern) throws SQLException {
		return super.getColumns(null, null, tableNamePattern, columnNamePattern);
	}
	
	/*@Override
	public ResultSet getColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		//log.info("getColumns() from: "+tableNamePattern); 
		//return null;
		return super.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
	}*/
	
	//XXX: add getPrimaryKeys()? unique indexes already returned by getIndexInfo()... 
	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			throws SQLException {
		//return super.getPrimaryKeys(null, null, table);
		return new EmptyResultSet();
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException {
		return getKeys(table, true);
	}
	
	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table)
			throws SQLException {
		return getKeys(table, false);
	}
	
	ResultSet getKeys(String table, boolean imported) throws SQLException {
		Connection conn = metadata.getConnection();
		
		String sql = "select null as PKTABLE_SCHEM, null as FKTABLE_SCHEM, null as PKTABLE_CAT, null as FKTABLE_CAT, null as FK_NAME, "
				+"szObject as FKTABLE_NAME, szColumn as FKCOLUMN_NAME, szReferencedObject as PKTABLE_NAME, szReferencedColumn as PKCOLUMN_NAME, "
				+"null as UPDATE_RULE, null as DELETE_RULE \n"
				+"from MSysRelationships";
		if(table!=null) {
			if(imported) {
				sql += "\nwhere szObject = '"+table+"' ";
			}
			else {
				sql += "\nwhere szReferencedObject = '"+table+"' ";
			}
		}
		
		try {
			PreparedStatement st = conn.prepareStatement(sql);
			log.debug("sql:\n"+sql);
			return st.executeQuery();
		}
		catch(SQLException e) {
			log.warn("error grabbing "+(imported?"imported":"exported")+" FKs [table='"+table+"']: "+e);
			if(isACCDB(conn.getMetaData())) {
				log.info("It seems than an '.accdb' database is in use. There is no support for grabbing relatioships (foreign keys) for this kind of database");
			}
			else {
				log.info("read permission is needed in table 'MSysRelationships' for sqldump to be able to grab relationships (foreign keys) from a '.mdb' database");
			}
			return new EmptyResultSet();
		}
	}
	
	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table,
			boolean unique, boolean approximate) throws SQLException {
		return super.getIndexInfo(null, null, table, unique, approximate);
	}

	//XXX: MSAccess: getTablePrivileges()?
	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		return new EmptyResultSet();
	}
	
	static final String ACCDB_PATTERN = "DBQ=.*\\.accdb";
	static final Pattern accdPattern = Pattern.compile(ACCDB_PATTERN);
	
	boolean isACCDB(DatabaseMetaData dbmd) throws SQLException {
		String url = dbmd.getURL();
		return accdPattern.matcher(url).find(); 
	}
}
