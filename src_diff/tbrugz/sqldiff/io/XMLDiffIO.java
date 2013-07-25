package tbrugz.sqldiff.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.DiffDumper;
import tbrugz.sqldiff.DiffGrabber;
import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.util.Utils;
import tbrugz.sqldump.util.XMLSerializer;

public class XMLDiffIO implements DiffGrabber, DiffDumper {

	static final String JAXB_DIFF_PACKAGES = "tbrugz.sqldump.dbmodel:tbrugz.sqldump.dbmsfeatures:tbrugz.sqldiff.model";

	static final Log log = LogFactory.getLog(XMLDiffIO.class);
	
	@Override
	public String type() {
		return "xml";
	}
	
	@Override
	public Diff grabDiff(Reader reader) throws JAXBException {
		XMLSerializer xmlser = new XMLSerializer(JAXB_DIFF_PACKAGES);
		SchemaDiff sdiff = (SchemaDiff) xmlser.unmarshal(reader);
		return sdiff;
	}
	
	@Override
	public Diff grabDiff(File file) throws JAXBException, FileNotFoundException {
		Diff diff = grabDiff(new FileReader(file));
		log.info(type()+" diff model grabbed from '"+file.getAbsolutePath()+"'");
		return diff;
	}

	@Override
	public void dumpDiff(Diff diff, Writer writer) throws JAXBException, IOException {
		XMLSerializer xmlser = new XMLSerializer(JAXB_DIFF_PACKAGES);
		xmlser.marshal(diff, writer);
		writer.close();
	}

	@Override
	public void dumpDiff(Diff diff, File file) throws JAXBException, IOException {
		Utils.prepareDir(file);
		dumpDiff(diff, new FileWriter(file));
		log.info(type()+" diff written to: "+file.getAbsolutePath());
	}

}
