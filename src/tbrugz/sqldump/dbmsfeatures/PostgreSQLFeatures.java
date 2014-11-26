package tbrugz.sqldump.dbmsfeatures;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.ExecutableParameter;
import tbrugz.sqldump.util.Utils;

public class PostgreSQLFeatures extends PostgreSQLAbstractFeatutres {

	static Log log = LogFactory.getLog(PostgreSQLFeatures.class);
	
	@Override
	String grabDBRoutinesQuery(String schemaPattern, String execNamePattern) {
		return "select routine_name, routine_type, data_type, external_language, routine_definition "
				+" , (select array_agg(parameter_name::text order by ordinal_position) from information_schema.parameters p where p.specific_name = r.specific_name) as parameter_names "
				+" , (select array_agg(data_type::text order by ordinal_position) from information_schema.parameters p where p.specific_name = r.specific_name) as parameter_types "
				+"from information_schema.routines r "
				+"where routine_definition is not null "
				+"and specific_schema = '"+schemaPattern+"' "
				+(execNamePattern!=null?"and routine_name = '"+execNamePattern+"' ":"")
				+"order by routine_catalog, routine_schema, routine_name ";
	}
	
	@Override
	public void grabDBExecutables(Collection<ExecutableObject> execs, String schemaPattern, String execNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing executables");
		String query = grabDBRoutinesQuery(schemaPattern, execNamePattern);
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		int rowcount = 0;
		while(rs.next()) {
			InformationSchemaRoutine eo = new InformationSchemaRoutine();
			eo.setSchemaName( schemaPattern );
			eo.setName( rs.getString(1) );
			try {
				eo.setType( DBObjectType.valueOf(Utils.normalizeEnumStringConstant(rs.getString(2))) );
			}
			catch(IllegalArgumentException iae) {
				log.warn("unknown object type: "+rs.getString(2));
				eo.setType( DBObjectType.EXECUTABLE );
			}
			ExecutableParameter ep = new ExecutableParameter();
			ep.setDataType(rs.getString(3));
			//eo.returnType = rs.getString(3);
			eo.setReturnParam(ep);
			
			eo.externalLanguage = rs.getString(4);
			eo.setBody( rs.getString(5) );
			
			//parameters!
			Array paramsArr = rs.getArray(6);
			if(paramsArr!=null) {
				String[] params = (String[]) paramsArr.getArray();
				//eo.parameterNames = Arrays.asList(params);
				String[] paramTypes = (String[]) rs.getArray(7).getArray();
				//eo.parameterTypes = Arrays.asList(paramTypes);
				List<ExecutableParameter> lpar = new ArrayList<ExecutableParameter>();
				for(int i=0;i<params.length;i++) {
					ExecutableParameter epar = new ExecutableParameter();
					epar.setName(params[i]);
					epar.setDataType(paramTypes[i]);
					lpar.add(epar);
				}
				if(lpar.size()>0) {
					eo.setParams(lpar);
				}
			}
			
			if(addExecutableToModel(execs, eo)) {
				count++;
			}
			
			rowcount++;
		}
		
		rs.close();
		st.close();
		log.info(count+" executable objects/routines grabbed [rowcount="+rowcount+"; all-executables="+execs.size()+"]");
	}
	
}
