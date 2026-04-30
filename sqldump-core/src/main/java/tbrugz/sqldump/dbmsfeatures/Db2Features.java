package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.QueryWithParams;
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
	@Override
	QueryWithParams grabDBTriggersQuery(String schemaPattern, String tableNamePattern, String triggerNamePattern) {
		List<Object> params = new ArrayList<>();
		String query = "select null trigger_catalog, TRIGSCHEMA trigger_schema, TRIGNAME trigger_name, TRIGEVENT event_manipulation, TABSCHEMA event_object_schema, "
			+"TABNAME event_object_table, TEXT action_statement, GRANULARITY action_orientation, TRIGTIME action_timing, null action_condition "
			+"\nfrom syscat.triggers "
			+"\nwhere TRIGSCHEMA = ? ";
		params.add(schemaPattern);
		if(tableNamePattern!=null) {
			query += "and TABNAME = ? ";
			params.add(tableNamePattern);
		}
		if(triggerNamePattern!=null) {
			query += "and TRIGNAME = ? ";
			params.add(triggerNamePattern);
		}
		query += "order by TRIGSCHEMA, TRIGNAME ";
		return new QueryWithParams(query, params);
	}
	
	/*
	 * select * from syscat.sequences
	 */
	@Override
	QueryWithParams grabDBSequencesQuery(String schemaPattern) {
		List<Object> params = new ArrayList<>();
		String query = "select SEQNAME sequence_name, MINVALUE minimum_value, increment, MAXVALUE maximum_value "
			+"\nfrom syscat.sequences "
			+"\nwhere SEQSCHEMA = ? "
			+"order by SEQSCHEMA, SEQNAME ";
		params.add(schemaPattern);
		return new QueryWithParams(query, params);
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
