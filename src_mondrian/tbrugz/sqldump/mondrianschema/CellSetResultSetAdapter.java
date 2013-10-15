package tbrugz.sqldump.mondrianschema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.olap4j.Axis;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.Position;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Member;

import tbrugz.sqldump.resultset.AbstractResultSet;
import tbrugz.sqldump.resultset.RSMetaDataAdapter;

public class CellSetResultSetAdapter extends AbstractResultSet implements ResultSet {

	static final Log log = LogFactory.getLog(CellSetResultSetAdapter.class);

	// CellSet properties
	final CellSet cellset;
	final CellSetAxis rowAxis;
	final CellSetAxis colAxis;
	
	// ResultSet properties
	final int colsFromRowAxis;
	final ResultSetMetaData metadata;
	
	public CellSetResultSetAdapter(CellSet cellset) {
		this.cellset = cellset;
		
		List<CellSetAxis> axis = cellset.getAxes();
		rowAxis = getAxisOfType(Axis.ROWS, axis);
		if(rowAxis==null) { throw new IllegalStateException("ROWS Axis must not be null"); }
		colAxis = getAxisOfType(Axis.COLUMNS, axis);
		if(colAxis==null) { throw new IllegalStateException("COLUMNS Axis must not be null"); }
		
		if(axis.size()>2) {
			List<String> axisTypes = new ArrayList<String>(); 
			for(CellSetAxis a: axis) {
				axisTypes.add(a.getAxisMetaData().getAxisOrdinal().toString());
			}
			log.warn("more than 2 axis detected [#"+axis.size()+"]: "+axisTypes);
		}
		
		List<String> colNames = new ArrayList<String>();
		
		for(Hierarchy h: rowAxis.getAxisMetaData().getHierarchies()) {
			//log.info("h: name="+h.getName()+" ; unique="+h.getUniqueName()+" ; caption="+h.getCaption());
			colNames.add(h.getCaption());
		}
		for(Position p: colAxis.getPositions()) {
			//log.info("  "+p.getOrdinal()+" : "+p.getMembers());
			colNames.add(p.getMembers().toString());
		}
		
		metadata = new RSMetaDataAdapter(null, null, colNames);
		
		rowCount = rowAxis.getPositionCount();
		colsFromRowAxis = rowAxis.getAxisMetaData().getHierarchies().size();
	}
	
	static CellSetAxis getAxisOfType(Axis axisType, List<CellSetAxis> axis) {
		for(CellSetAxis ax: axis) {
			if(ax.getAxisOrdinal().equals(axisType)) {
				return ax;
			}
		}
		return null;
	}
	
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metadata;
	}

	//--------------- navigation methods
	
	int position = -1;
	int rowCount = 0;
	
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
		if(rowCount>=row) {
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
		if(rowCount-1 > position) {
			position++;
			updateCurrentElement();
			return true;
		}
		return false;
	}
	
	void updateCurrentElement() {
		/*if(position<0) {
			currentNonPivotKey = null;
			return;
		}
		currentNonPivotKey = nonPivotKeyValues.get(position);*/
	}
	
	void resetPosition() {
		position = -1;
		updateCurrentElement();
	}
	
	//--------------- value getter methods
	
	@Override
	public Object getObject(int columnIndex) throws SQLException {
		//log.info("getObject: "+columnIndex +" "+ colsFromRowAxis); 
		if(columnIndex <= colsFromRowAxis) {
			//Hierarchy hier = rowAxis.getAxisMetaData().getHierarchies().get(columnIndex-1);
			//List<Member> members = rowAxis.getPositions().get(position).getMembers();
			//log.info("members["+position+";"+columnIndex+"]: "+members);
			Member member = rowAxis.getPositions().get(position).getMembers().get(columnIndex-1);
			return member.getCaption();
		}
		Position rowP = rowAxis.getPositions().get(position);
		int rowOrdinal = rowP.getOrdinal();
		Position colP = colAxis.getPositions().get(columnIndex-colsFromRowAxis-1);
		int colOrdinal = colP.getOrdinal();
		
		try {
			Cell c = cellset.getCell(colP, rowP);
			return c.getValue();
		}
		catch(IndexOutOfBoundsException e) {
			log.warn("error["+rowOrdinal+"/"+colOrdinal+"]: "+e);
			return null;
			//return rowOrdinal+"/"+colOrdinal;
		}
	}
	
	@Override
	public String getString(String columnLabel) throws SQLException {
		Object o = getObject(columnLabel);
		if(o==null) { return null; }
		return String.valueOf(o);
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		if(o==null) { return null; }
		return String.valueOf(o);
	}
	
}
