package tbrugz.sqldump.util;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class QueryWithParams {

	final String query;
	final List<Object> params;
	
	public QueryWithParams(String query, List<Object> params) {
		this.query = query;
		this.params = params;
	}
	
	public String getQuery() {
		return query;
	}
	
	public List<Object> getParams() {
		return params;
	}
	
	public void setParameters(PreparedStatement st) throws SQLException {
		if(params==null) { return; }
		
		for(int i=0;i<params.size();i++) {
			st.setObject(i+1, params.get(i));
		}
	}
	
	@Override
	public String toString() {
		return "["+query+";params="+params+"]";
	}

}
