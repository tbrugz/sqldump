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
import org.olap4j.OlapConnection;
import org.olap4j.layout.CellSetFormatter;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.parser.MdxParser;
import org.olap4j.mdx.parser.MdxParserFactory;
import org.olap4j.mdx.parser.MdxValidator;

import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.Utils;

public class Olap4jMDXQueries extends AbstractSQLProc {

	static final Log log = LogFactory.getLog(Olap4jMDXQueries.class);
	
	static final String PREFIX_MDXQUERIES = "sqldump.mdxqueries";
	static final String PROP_MDXQUERIES_IDS = PREFIX_MDXQUERIES+".ids";
	static final String SUFFIX_MDXQUERIES_QUERY = ".query";
	static final String SUFFIX_MDXQUERIES_NAME = ".name";
	static final String SUFFIX_MDXQUERIES_OUTFILEPATTERN = ".outfilepattern";

	OlapConnection olapConnection = null;
	MdxParser parser = null;
	MdxValidator validator = null;
	//DataDump dd = new DataDump();
	
	//XXX: prop to parse & validate query
	boolean validate = true;
	String fileOutputPattern = null;
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		
		fileOutputPattern = prop.getProperty(PREFIX_MDXQUERIES+SUFFIX_MDXQUERIES_OUTFILEPATTERN);
	}
	
	@Override
	public void process() {
		if(!Olap4jUtil.isOlapConnection(conn)) {
			log.warn("connection is not instance of OlapConnection");
			return;
		}
		if(fileOutputPattern==null) {
			log.warn("no outfilepattern defined (suffix '"+SUFFIX_MDXQUERIES_OUTFILEPATTERN+"')");
			return;
		}
		
		olapConnection = (OlapConnection) conn;
		MdxParserFactory pFactory = olapConnection.getParserFactory();
		parser = pFactory.createMdxParser(olapConnection);
		validator = pFactory.createMdxValidator(olapConnection);
		
		List<String> queryIds = Utils.getStringListFromProp(prop, PROP_MDXQUERIES_IDS, ",");
		if(queryIds==null) {
			log.warn("no mdx queries defined (prop '"+PROP_MDXQUERIES_IDS+"')");
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
					log.warn("null query ["+id+"]");
				}
				else {
					Writer w = co.getCategorizedWriter(queryName!=null?queryName:id);
					dumpQuery(id, query, w);
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

	//TODOne: add COut
	void dumpQuery(String id, String query, Writer w) throws SQLException, IOException {
		if(validate) {
			log.info("validating query '"+id+"'");
			SelectNode parsedObject = parser.parseSelect(query);
			validator.validateSelect(parsedObject);
		}
		
		long initTime = System.currentTimeMillis();
		log.info("mdx query ["+id+"]: "+query);
		CellSet cellSet = olapConnection.prepareOlapStatement(query).executeQuery();
		
		/*
		dd.dumpResultSet(cellSet, prop, id, id,
				null, //charset, XXX?
				0, //rowlimit, 
				null, //syntaxList, XXX?
				null, //partitionByPatterns, XXX?
				null, //keyColumns,
				null, //importedFKs,
				null, //uniqueKeys,
				null, //rsDecoratorFactory,
				initTime //initTime
				);
		*/
		CellSetFormatter formatter = new RectangularCellSetFormatter(false);
		formatter.format(cellSet, new PrintWriter(w));
		//w.close();
		log.info("query ["+id+"] dumped [elapsed="+(System.currentTimeMillis()-initTime)+"ms]");
		//formatter.format(cellSet, new PrintWriter(System.out, true));
	}
}
