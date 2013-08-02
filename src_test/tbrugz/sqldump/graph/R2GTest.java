package tbrugz.sqldump.graph;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import tbrugz.sqldump.XMLUtil;
import tbrugz.sqldump.def.Processor;
import tbrugz.sqldump.util.ConnectionUtil;

public class R2GTest {

	@Test
	public void testR2G() throws SAXException, ParserConfigurationException, IOException, ClassNotFoundException, SQLException, NamingException {
		Properties prop = new Properties();
		prop.load(R2GTest.class.getResourceAsStream("r2g.properties"));
		Connection conn = ConnectionUtil.initDBConnection("sqldump", prop);
		
		Processor proc = new ResultSet2GraphML();
		proc.setProperties(prop);
		proc.setConnection(conn);
		proc.process();
		
		/*
		//dom4j...
		Document dom = new SAXReader().read("work/output/graph/r2g.graphml");
		Element e = dom.getRootElement();
		System.out.println(dom.getName()+" / "+dom.getNodeType()+" / "+dom.nodeCount());
		System.out.println(dom.asXML());
		*/
		Document doc = XMLUtil.getDoc(new File("work/output/graph/r2g.graphml"));
		
		//List ln = e.selectNodes("//node");//graphml/graph/node
		NodeList ln = XMLUtil.getList(doc, "node");
		Assert.assertEquals(2, ln.getLength());
		
		//List le = dom.selectNodes("//edge");
		NodeList le = XMLUtil.getList(doc, "edge");
		Assert.assertEquals(3, le.getLength());
	}

	@Test
	public void testR2Gsql() throws SAXException, ParserConfigurationException, IOException, ClassNotFoundException, SQLException, NamingException {
		Properties prop = new Properties();
		prop.load(R2GTest.class.getResourceAsStream("r2g.properties"));
		prop.setProperty("sqldump.graphmlqueries", "q2");
		Connection conn = ConnectionUtil.initDBConnection("sqldump", prop);
		
		Processor proc = new ResultSet2GraphML();
		proc.setProperties(prop);
		proc.setConnection(conn);
		proc.process();
		
		Document doc = XMLUtil.getDoc(new File("work/output/graph/r2g-sql.graphml"));
		
		//List ln = e.selectNodes("//node");//graphml/graph/node
		NodeList ln = XMLUtil.getList(doc, "node");
		Assert.assertEquals(3, ln.getLength());
		
		//List le = dom.selectNodes("//edge");
		NodeList le = XMLUtil.getList(doc, "edge");
		Assert.assertEquals(4, le.getLength());
	}
}
