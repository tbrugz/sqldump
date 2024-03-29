package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.SchemaModel;

/*
 * see:
 * http://www.monetdb.org/Documentation/SQLcatalog
 * https://www.monetdb.org/documentation-Jan2022/user-guide/sql-catalog/functions-arguments-types/
 */
public class MonetDBFeatures extends InformationSchemaFeatures {
	static Log log = LogFactory.getLog(MonetDBFeatures.class);
	
	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		
		grabCheckConstraints = false; //XXX: check constraints?
		grabUniqueConstraints = false; //XXX: unique constraints - from SYS.KEYS [type==1]? (pk==0, fk==2)
		grabSynonyms = false; //monetdb doesn't seem to have synonyms
		grabTriggers = false; //XXX: grab from SYS.TRIGGERS
	}
	
	public void grabDBObjects(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		try {
			conn.rollback();
		}
		catch(SQLException e) {
			log.warn("sqlexception: "+e);
		}
		super.grabDBObjects(model, schemaPattern, conn);
	}
	
	String grabDBViewsQuery(String schemaPattern) {
		return "select '' as table_catalog, schemas.name as table_schema, tables.name as table_name"
			+", query as view_definition, 'NONE' as check_option, 'NO' as is_updatable "
			+"from sys.tables inner join sys.schemas on tables.schema_id = schemas.id "
			+"where query is not null "
			+"and schemas.name = '"+schemaPattern+"' "
			+"order by schemas.name, tables.name ";
	}

	String grabDBSequencesQuery(String schemaPattern) {
		return "select sequences.name as sequence_name, sequences.\"minvalue\" as minimum_value, \"increment\", \"maxvalue\" as maximum_value "
				//+", start, cacheinc, cycle "
				+"from sys.sequences inner join sys.schemas on sequences.schema_id = schemas.id "
				+"where schemas.name = '"+schemaPattern+"' "
				+"order by sequences.name ";
	}
	
	String grabDBRoutinesQuery(String schemaPattern) {
		return "select functions.name as routine_name, 'FUNCTION' as routine_type, ret.type as data_type"
				//+", functions.mod as external_language, func as routine_definition"
				+", null as external_language, func as routine_definition, null as external_name "
				+", null as is_deterministic " //XXX: use 'functions.side_effect'?
				+", p.name as parameter_name, p.type as data_type, p.number as ordinal_position "
				+"from sys.functions "
				+"inner join sys.schemas on functions.schema_id = schemas.id "
				+"inner join sys.args p on functions.id = p.func_id "
				+"inner join sys.args ret on functions.id = ret.func_id "
				+"where schemas.name = '"+schemaPattern+"' "
				+"and p.number <> 0 "
				+"and ret.number = 0 "
				+"order by functions.name, p.number ";
	}
	
}
