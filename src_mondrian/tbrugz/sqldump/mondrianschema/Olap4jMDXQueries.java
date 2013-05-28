package tbrugz.sqldump.mondrianschema;

import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.layout.CellSetFormatter;
import org.olap4j.layout.RectangularCellSetFormatter;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.parser.MdxParser;
import org.olap4j.mdx.parser.MdxParserFactory;
import org.olap4j.mdx.parser.MdxValidator;

import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.util.Utils;

public class Olap4jMDXQueries extends AbstractSQLProc {

	static final Log log = LogFactory.getLog(Olap4jMDXQueries.class);
	
	static final String PREFIX_MDXQUERIES = "sqldump.mdxqueries";
	static final String PROP_MDXQUERIES_IDS = PREFIX_MDXQUERIES+".ids";
	static final String SUFFIX_MDXQUERIES_QUERY = ".query";

	OlapConnection olapConnection = null;
	MdxParser parser = null;
	MdxValidator validator = null;
	
	//XXX: prop to parse & validate query
	boolean validate = true;
	
	@Override
	public void process() {
		if(!Olap4jUtil.isOlapConnection(conn)) {
			log.warn("connection is not instance of OlapConnection");
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

		for(String id: queryIds) {
			try {
				String query = prop.getProperty(PREFIX_MDXQUERIES+"."+id+SUFFIX_MDXQUERIES_QUERY);
				dumpQuery(id, query);
			}
			catch (OlapException e) {
				log.warn("ex: "+e);
				if(failonerror) {
					throw new ProcessingException(e);
				}
			}
		}
	}

	//TODO: add COut
	void dumpQuery(String id, String query) throws OlapException {
		if(validate) {
			log.info("validating query '"+id+"'");
			SelectNode parsedObject = parser.parseSelect(query);
			validator.validateSelect(parsedObject);
		}
		
		log.info("mdx query ["+id+"]: "+query);
		CellSet cellSet = olapConnection.prepareOlapStatement(query).executeQuery();
		CellSetFormatter formatter = new RectangularCellSetFormatter(false);
		formatter.format(cellSet, new PrintWriter(System.out, true));
	}
}
