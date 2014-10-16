package tbrugz.sqldiff;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Properties;

import javax.xml.bind.JAXBException;

import tbrugz.sqldiff.model.Diff;

public interface DiffDumper {
	String type();
	void setProperties(Properties prop);
	void dumpDiff(Diff diff, Writer writer) throws JAXBException, IOException;
	void dumpDiff(Diff diff, File file) throws JAXBException, IOException;
}
