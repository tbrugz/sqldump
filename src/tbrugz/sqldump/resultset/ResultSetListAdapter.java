package tbrugz.sqldump.resultset;

import java.beans.IntrospectionException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ResultSetListAdapter<E extends Object> extends BaseResultSetCollectionAdapter<E> {
	static final Log log = LogFactory.getLog(ResultSetListAdapter.class);

	final List<E> list;
	int position;

	public ResultSetListAdapter(String name, List<String> uniqueCols, List<E> list, Class<? extends E> clazz) throws IntrospectionException {
		this(name, uniqueCols, null, list, clazz);
	}
	
	public ResultSetListAdapter(String name, List<String> uniqueCols, List<String> allCols, List<E> list, Class<? extends E> clazz) throws IntrospectionException {
		super(name, uniqueCols, allCols, clazz);
		this.list = list;
		//Initially the cursor is positioned before the first row
		resetPosition();
	}
	
	@Override
	public int getType() throws SQLException {
		return ResultSet.TYPE_SCROLL_SENSITIVE;
	}
	
	@Override
	public void beforeFirst() throws SQLException {
		resetPosition();
	}
	
	@Override
	public boolean first() throws SQLException {
		resetPosition();
		return next();
	}
	
	@Override
	public boolean absolute(int row) throws SQLException {
		if(list.size()>=row) {
			position = row-1;
			updateCurrentElement();
			return true;
		}
		return false;
	}
	
	@Override
	public boolean relative(int rows) throws SQLException {
		int newpos = position + rows + 1;
		if(newpos>0) { return absolute(newpos); }
		return false;
	}

	@Override
	public boolean next() throws SQLException {
		if(list.size()-1 > position) {
			position++;
			updateCurrentElement();
			return true;
		}
		return false;
	}
	
	void updateCurrentElement() {
		currentElement = list.get(position);
	}
	
	void resetPosition() {
		//XXX: should reset to 0? position==1 should point to the first element?
		position = -1;
	}

}
