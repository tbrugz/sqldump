package tbrugz.sqldump.mondrianschema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.olap4j.Axis;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.Position;
import org.olap4j.metadata.Hierarchy;

import tbrugz.sqldump.resultset.AbstractResultSet;
import tbrugz.sqldump.resultset.RSMetaDataAdapter;

public class CellSetResultSetAdapter extends AbstractResultSet implements ResultSet {

	static final Log log = LogFactory.getLog(CellSetResultSetAdapter.class);

	final CellSet cellset;

	final ResultSetMetaData metadata;
	
	public CellSetResultSetAdapter(CellSet cellset) {
		this.cellset = cellset;
		
		List<CellSetAxis> axis = cellset.getAxes();
		CellSetAxis rowAxis = getAxisOfType(Axis.ROWS, axis);
		if(rowAxis==null) { throw new IllegalStateException("ROWS Axis must not be null"); }
		CellSetAxis colAxis = getAxisOfType(Axis.COLUMNS, axis);
		if(colAxis==null) { throw new IllegalStateException("COLUMNS Axis must not be null"); }
		
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
}
