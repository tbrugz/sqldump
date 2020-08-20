package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;

public class PrestoDbFeatures extends InformationSchemaFeatures {

	private static final Log log = LogFactory.getLog(PrestoDbFeatures.class);
	
	@Override
	String grabDBViewsQuery(String schemaPattern, String viewNamePattern) {
		return "select table_catalog, table_schema, table_name, view_definition "
			+"\nfrom "+informationSchema+".views "
			+"\nwhere view_definition is not null "
			+"and table_schema = '"+schemaPattern+"' "
			+(viewNamePattern!=null?"and table_name = '"+viewNamePattern+"' ":"")
			+"order by table_catalog, table_schema, table_name ";
	}

	@Override
	public void grabDBTriggers(Collection<Trigger> triggers,
			String schemaPattern, String tableNamePattern,
			String triggerNamePattern, Connection conn) throws SQLException {
		log.warn("grabDBTriggers: not implemented");
	}
	
	@Override
	public void grabDBExecutables(Collection<ExecutableObject> execs,
			String schemaPattern, String execNamePattern, Connection conn)
			throws SQLException {
		log.warn("grabDBExecutables: not implemented");
	}
	
	@Override
	public void grabDBSequences(Collection<Sequence> seqs, String schemaPattern,
			String sequenceNamePattern, Connection conn) throws SQLException {
		log.warn("grabDBSequences: not implemented");
	}
	
	@Override
	public void grabDBCheckConstraints(Collection<Table> tables,
			String schemaPattern, String tableNamePattern, String constraintNamePattern, Connection conn)
			throws SQLException {
		log.warn("grabDBCheckConstraints: not implemented");
	}
	
	@Override
	public void grabDBUniqueConstraints(Collection<Table> tables,
			String schemaPattern, String tableNamePattern, String constraintNamePattern, Connection conn)
			throws SQLException {
		log.warn("grabDBUniqueConstraints: not implemented");
	}

}
