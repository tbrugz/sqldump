package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.util.Utils;

public class PostgreSQLFeatures extends InformationSchemaFeatures {

	@Override
	String grabDBRoutinesQuery(String schemaPattern) {
		return "select routine_name, routine_type, data_type, external_language, routine_definition "
				+" ,(select array_agg(parameter_name||' '||data_type order by ordinal_position) from information_schema.parameters p where p.specific_name = r.specific_name) as parameter_names "
				+"from information_schema.routines r "
				+"where routine_definition is not null "
				+"and specific_schema = '"+schemaPattern+"' "
				+"order by routine_catalog, routine_schema, routine_name ";
	}
	
	@Override
	void grabDBRoutines(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing executables");
		String query = grabDBRoutinesQuery(schemaPattern);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			InformationSchemaRoutine eo = new InformationSchemaRoutine();
			eo.setSchemaName( schemaPattern );
			eo.name = rs.getString(1);
			try {
				eo.type = DBObjectType.valueOf(Utils.normalizeEnumStringConstant(rs.getString(2)));
			}
			catch(IllegalArgumentException iae) {
				log.warn("unknown object type: "+rs.getString(2));
				eo.type = DBObjectType.EXECUTABLE;
			}
			eo.returnType = rs.getString(3);
			eo.externalLanguage = rs.getString(4);
			eo.body = rs.getString(5);
			
			//parameters!
			String[] params = (String[]) rs.getArray(6).getArray();
			StringBuffer sb = new StringBuffer();
			for(int i=0;i<params.length;i++) {
				if(i>0) { sb.append(", "); }
				sb.append(params[i]);
			}
			eo.parameters = sb.toString();
			
			model.getExecutables().add(eo);
			count++;
		}
		
		rs.close();
		st.close();
		log.info(count+" executable objects/routines grabbed");
	}
	
}
