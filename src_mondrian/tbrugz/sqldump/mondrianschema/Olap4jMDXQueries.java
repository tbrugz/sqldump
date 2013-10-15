package tbrugz.sqldump.mondrianschema;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.OlapConnection;
import org.olap4j.Position;
import org.olap4j.layout.CellSetFormatter;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.parser.MdxParser;
import org.olap4j.mdx.parser.MdxParserFactory;
import org.olap4j.mdx.parser.MdxValidator;
import org.olap4j.metadata.Hierarchy;

import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.sqlrun.QueryDumper;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.Utils;

/*
 * TODOne: add ResultSetCellSetAdapter
 * XXX: add csv exporter?
 * TODO: add option to use different dump syntaxes
 */
public class Olap4jMDXQueries extends AbstractSQLProc {

	static final Log log = LogFactory.getLog(Olap4jMDXQueries.class);
	
	static final String PREFIX_MDXQUERIES = "sqldump.mdxqueries";
	static final String PROP_MDXQUERIES_IDS = PREFIX_MDXQUERIES+".ids";
	static final String SUFFIX_MDXQUERIES_QUERY = ".query";
	static final String SUFFIX_MDXQUERIES_NAME = ".name";
	static final String SUFFIX_MDXQUERIES_OUTFILEPATTERN = ".outfilepattern";
	static final String SUFFIX_MDXQUERIES_VALIDATE = ".validate";
	static final String SUFFIX_MDXQUERIES_USE_CELLSET_FORMATTER = ".use-cellset-formatter";
	static final String SUFFIX_MDXQUERIES_X_COMPACT = ".x-compactmode";

	OlapConnection olapConnection = null;
	MdxParser parser = null;
	MdxValidator validator = null;
	
	//XXX: prop to parse & validate query
	boolean validate = true;
	String fileOutputPattern = null;
	boolean useCellSetFormatter = false;
	boolean compactMode = false;
	boolean mayCreateOlapConnection = true; // add prop for mayCreateOlapConnection?
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		
		fileOutputPattern = prop.getProperty(PREFIX_MDXQUERIES+SUFFIX_MDXQUERIES_OUTFILEPATTERN);
		validate = Utils.getPropBool(prop, PREFIX_MDXQUERIES+SUFFIX_MDXQUERIES_VALIDATE, validate);
		useCellSetFormatter = Utils.getPropBool(prop, PREFIX_MDXQUERIES+SUFFIX_MDXQUERIES_USE_CELLSET_FORMATTER, useCellSetFormatter);
		compactMode = Utils.getPropBool(prop, PREFIX_MDXQUERIES+SUFFIX_MDXQUERIES_X_COMPACT, compactMode);
	}
	
	@Override
	public void process() {
		if(!Olap4jUtil.isOlapConnection(conn) && mayCreateOlapConnection) {
			Olap4jConnector connector = new Olap4jConnector();
			connector.setFailOnError(failonerror);
			connector.setProperties(prop);
			//connector.setSchemaModel(model);
			connector.setConnection(conn);
			connector.process();
			conn = connector.getConnection();
		}
		if(!Olap4jUtil.isOlapConnection(conn)) {
			String message = "connection is not instance of OlapConnection";
			log.warn(message);
			if(failonerror) {
				throw new ProcessingException(message);
			}
			return;
		}
		
		if(fileOutputPattern==null) {
			String message = "no '"+SUFFIX_MDXQUERIES_OUTFILEPATTERN+"' prop defined (prefix is '"+PREFIX_MDXQUERIES+"')";
			log.warn(message);
			if(failonerror) {
				throw new ProcessingException(message);
			}
			return;
		}
		
		olapConnection = (OlapConnection) conn;
		MdxParserFactory pFactory = olapConnection.getParserFactory();
		parser = pFactory.createMdxParser(olapConnection);
		validator = pFactory.createMdxValidator(olapConnection);
		
		List<String> queryIds = Utils.getStringListFromProp(prop, PROP_MDXQUERIES_IDS, ",");
		if(queryIds==null) {
			String message = "no mdx queries defined (prop '"+PROP_MDXQUERIES_IDS+"')";
			log.warn(message);
			if(failonerror) {
				throw new ProcessingException(message);
			}
			return;
		}
		
		String finalPattern = CategorizedOut.generateFinalOutPattern(fileOutputPattern, 
				new String[]{Defs.addSquareBraquets(Defs.PATTERN_TABLENAME)}
				);
		CategorizedOut co = new CategorizedOut(finalPattern);

		int count = 0;
		for(String id: queryIds) {
			try {
				String query = prop.getProperty(PREFIX_MDXQUERIES+"."+id+SUFFIX_MDXQUERIES_QUERY);
				String queryName = prop.getProperty(PREFIX_MDXQUERIES+"."+id+SUFFIX_MDXQUERIES_NAME);
				if(query==null) {
					String message = "null query '"+id+"'";
					log.warn(message);
					if(failonerror) {
						throw new ProcessingException(message);
					}
					continue;
				}
				
				String qid = queryName!=null?queryName:id;
				CellSet cellset = executeQuery(qid, query);
				
				if(useCellSetFormatter) {
					Writer w = co.getCategorizedWriter(qid);
					dumpQueryCellSetFormatter(qid, cellset, w);
					CategorizedOut.closeWriter(w);
				}
				else {
					// using cellset->resultset adapter 
					//dumpQueryResultSetAdapter(qid, cellset);
					Writer w = co.getCategorizedWriter(qid);
					dumpQueryResultSetAdapter(qid, cellset, w);
					CategorizedOut.closeWriter(w);
				}
				
				count++;
			}
			catch (Exception e) {
				log.warn("ex: "+e);
				if(failonerror) {
					throw new ProcessingException(e);
				}
			}
		}
		log.info(count+" queries [of "+queryIds.size()+"] dumped");
	}

	CellSet executeQuery(String id, String query) throws SQLException, IOException {
		if(validate) {
			log.info("validating query '"+id+"'");
			SelectNode parsedObject = parser.parseSelect(query);
			validator.validateSelect(parsedObject);
		}
		
		long initTime = System.currentTimeMillis();
		log.info("mdx query ["+id+"]: "+query);
		CellSet cellSet = olapConnection.prepareOlapStatement(query).executeQuery();
		//log.info("cellSet: "+cellSet.getClass()+" / "+cellSet.getMetaData().getClass());
		//logCellSetInfo(cellSet);
		log.info("query '"+id+"' returned [elapsed="+(System.currentTimeMillis()-initTime)+"ms]");
		return cellSet;
	}
	
	void dumpQueryCellSetFormatter(String id, CellSet cellSet, Writer w) throws SQLException, IOException {
		long initTime = System.currentTimeMillis();
		
		CellSetFormatter formatter = new RectangularCellSetFormatter(compactMode);
		//CellSetFormatter formatter = new TraditionalCellSetFormatter();
		formatter.format(cellSet, new PrintWriter(w));
		
		log.info("query '"+id+"' dumped [CellSetFormatter;elapsed="+(System.currentTimeMillis()-initTime)+"ms]");
		//formatter.format(cellSet, new PrintWriter(System.out, true));
	}
	
	void dumpQueryResultSetAdapter(String id, CellSet cellSet) throws SQLException, IOException {
		long initTime = System.currentTimeMillis();
		CellSetResultSetAdapter csrsad = new CellSetResultSetAdapter(cellSet);
		QueryDumper.simplerRSDump(csrsad);
		log.info("query '"+id+"' dumped [ResultSetAdapter.simplerRSDump;elapsed="+(System.currentTimeMillis()-initTime)+"ms]");
	}

	void dumpQueryResultSetAdapter(String id, CellSet cellSet, Writer w) throws SQLException, IOException {
		long initTime = System.currentTimeMillis();
		CellSetResultSetAdapter csrsad = new CellSetResultSetAdapter(cellSet);
		QueryDumper.simpleRSDump(csrsad, "FFCDataDump", prop, w);
		log.info("query '"+id+"' dumped [ResultSetAdapter;elapsed="+(System.currentTimeMillis()-initTime)+"ms]");
	}
	
	void logCellSetInfo(CellSet cellSet) {
		List<CellSetAxis> axis = cellSet.getAxes();
		log.info("axis: "+axis);
		for(CellSetAxis ax: axis) {
			log.info(ax.getAxisOrdinal()+" ; "+ax.getPositionCount());
			for(Hierarchy h: ax.getAxisMetaData().getHierarchies()) {
				log.info("h: name="+h.getName()+" ; unique="+h.getUniqueName()+" ; caption="+h.getCaption());
			}
			for(Position p: ax.getPositions()) {
				log.info("  "+p.getOrdinal()+" : "+p.getMembers());
			}
		}
	}
}
