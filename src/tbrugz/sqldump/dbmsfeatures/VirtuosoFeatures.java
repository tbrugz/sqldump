package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.SchemaModel;

public class VirtuosoFeatures extends InformationSchemaFeatures {
	static final Log log = LogFactory.getLog(VirtuosoFeatures.class);

	String grabDBViewsQuery(String schemaPattern) {
		return "select table_catalog, table_schema, table_name, view_definition, check_option, is_updatable "
			+"from information_schema.views "
			+"where view_definition is not null "
			+"and table_schema = '"+schemaPattern+"' ";
			//+"order by table_catalog, table_schema, table_name ";
		
		// XXX order by: possible bug?
		// http://www.mail-archive.com/virtuoso-users@lists.sourceforge.net/msg03126.html
		// http://boards.openlinksw.com/phpBB3/viewtopic.php?f=12&t=776&sid=1486d8cf5e5c9880ac174718b9d5e0ea&start=10
	}
	
	@Override
	public void grabDBTriggers(SchemaModel model, String schemaPattern, String tableNamePattern, String triggerNamePattern, Connection conn)
			throws SQLException {
		//XXX: use DBA.sys_triggers ?
		log.warn("grabTriggers: not implemented");
	}
	
	@Override
	public void grabDBExecutables(SchemaModel model, String schemaPattern, String execNamePattern, Connection conn)
			throws SQLException {
		log.warn("grabDBRoutines: not implemented");
	}
	
	@Override
	public void grabDBSequences(SchemaModel model, String schemaPattern, String sequenceNamePattern,
			Connection conn) throws SQLException {
		log.warn("grabDBSequences: not implemented");
	}
	
	@Override
	public void grabDBCheckConstraints(SchemaModel model, String schemaPattern, String constraintNamePattern,
			Connection conn) throws SQLException {
		log.warn("grabDBCheckConstraints: not implemented");
	}

	@Override
	String grabDBUniqueConstraintsQuery(String schemaPattern, String constraintNamePattern) {
		return "select tc.constraint_schema, tc.table_name, tc.constraint_name, column_name " 
				+"from information_schema.table_constraints tc, information_schema.key_column_usage kcu "
				+"where tc.constraint_name = kcu.constraint_name "
				+"and tc.constraint_schema = '"+schemaPattern+"' "
				+"and v_key_is_main = 0 "
				+(constraintNamePattern!=null?"and tc.constraint_name = '"+constraintNamePattern+"' ":"")
				+"order by tc.table_name, tc.constraint_name, ordinal_position ";
	}
}
