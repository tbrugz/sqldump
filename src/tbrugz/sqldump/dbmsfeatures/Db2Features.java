package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Table;

/*
 * SYSIBMADM - similar to oracle metadata tables?
 * SYSSTAT - statistics?
 */
public class Db2Features extends InformationSchemaFeatures {
	static Log log = LogFactory.getLog(Db2Features.class);
	
	public Db2Features() {
		setInformationSchemaName("sysibm");
	}
	
	/*
	 * select * from syscat.triggers
	 * 
	 * select * from sysibm.systriggers - similar to derby?
	 */
	String grabDBTriggersQuery(String schemaPattern, String tableNamePattern, String triggerNamePattern) {
		return "select null trigger_catalog, TRIGSCHEMA trigger_schema, TRIGNAME trigger_name, TRIGEVENT event_manipulation, TABSCHEMA event_object_schema, "
			+"TABNAME event_object_table, TEXT action_statement, GRANULARITY action_orientation, TRIGTIME action_timing, null action_condition "
			+"\nfrom syscat.triggers "
			+"\nwhere TRIGSCHEMA = '"+schemaPattern+"' "
			+(tableNamePattern!=null?"and TABNAME = '"+tableNamePattern+"' ":"")
			+(triggerNamePattern!=null?"and TRIGNAME = '"+triggerNamePattern+"' ":"")
			+"order by TRIGSCHEMA, TRIGNAME ";
	}
	
	/*
	 * select * from syscat.sequences
	 */
	@Override
	String grabDBSequencesQuery(String schemaPattern) {
		return "select SEQNAME sequence_name, MINVALUE minimum_value, increment, MAXVALUE maximum_value "
			+"\nfrom syscat.sequences "
			+"\nwhere SEQSCHEMA = '"+schemaPattern+"' "
			+"order by SEQSCHEMA, SEQNAME ";
	}
	
	/*
	 * select * from sysibm.check_constraints
	 * ?? select * from ?.constraint_column_usage
	 */
	@Override
	public void grabDBCheckConstraints(Collection<Table> tables,
			String schemaPattern, String tableNamePattern, String constraintNamePattern, Connection conn)
			throws SQLException {
		log.warn("grabDBCheckConstraints: not implemented");
	}

	/*
	 * select * from sysibm.table_constraints
	 * ?? select * from ?.constraint_column_usage
	 */
	@Override
	public void grabDBUniqueConstraints(Collection<Table> tables,
			String schemaPattern, String tableNamePattern, String constraintNamePattern, Connection conn)
			throws SQLException {
		log.warn("grabDBUniqueConstraints: not implemented");
	}

}
