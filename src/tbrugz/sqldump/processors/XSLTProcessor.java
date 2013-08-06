package tbrugz.sqldump.processors;

import java.io.InputStream;
import java.io.Writer;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.AbstractProcessor;
import tbrugz.sqldump.def.ProcessingException;

public abstract class XSLTProcessor extends AbstractProcessor {
	static Log log = LogFactory.getLog(XSLTProcessor.class);

	protected InputStream xsl;
	
	protected InputStream in;
	protected Writer out;
	
	@Override
	public void process() {
		try {
			processInternal();
		} catch (TransformerException e) {
			log.warn("error: "+e);
			if(failonerror) {
				throw new ProcessingException(e);
			}
		}
	}
	
	protected void processInternal() throws TransformerException {
		if(xsl==null) {
			throw new ProcessingException("xsl can't be null");
		}
		TransformerFactory factory = TransformerFactory.newInstance();
		Source xslt = new StreamSource(xsl);
		Transformer transformer = factory.newTransformer(xslt);
		
		Source text = new StreamSource(in);
		transformer.transform(text, new StreamResult(out));		
	}
	
	@Override
	public boolean needsConnection() {
		return false;
	}
	
}
