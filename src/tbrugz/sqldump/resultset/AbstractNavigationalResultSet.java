package tbrugz.sqldump.resultset;

import java.sql.ResultSet;
import java.sql.SQLException;

import tbrugz.sqldump.resultset.AbstractResultSet;

public abstract class AbstractNavigationalResultSet extends AbstractResultSet {

	protected abstract void updateCurrentElement();
	
	protected abstract int getRowCount();

	protected abstract int getPosition();

	protected abstract void setPosition(int position);
	
	void incrementPosition() {
		setPosition(getPosition()+1);
	}
	
	//=============== RS methods - navigation ===============
	
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
		//resetPosition();
		beforeFirst();
		return next();
	}
	
	@Override
	public boolean absolute(int row) throws SQLException {
		if(getRowCount()>=row) {
			setPosition(row-1);
			updateCurrentElement();
			return true;
		}
		return false;
	}
	
	@Override
	public boolean relative(int rows) throws SQLException {
		int newpos = getPosition() + rows + 1;
		if(newpos>0) { return absolute(newpos); }
		return false;
	}

	@Override
	public boolean next() throws SQLException {
		if(getRowCount()-1 > getPosition()) {
			//setPosition(getPosition()+1);
			incrementPosition();
			updateCurrentElement();
			return true;
		}
		return false;
	}
	
	void resetPosition() {
		setPosition(-1);
		updateCurrentElement();
	}
	
	//=============== / RS methods - navigation ============

}
