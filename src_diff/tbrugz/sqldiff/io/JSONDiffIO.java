package tbrugz.sqldiff.io;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import javax.xml.bind.JAXBException;

import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.util.JSONSerializer;

public class JSONDiffIO extends XMLDiffIO {

	@Override
	public String type() {
		return "json";
	}
	
	@Override
	public Diff grabDiff(Reader reader) {
		try {
			JSONSerializer ser = new JSONSerializer(JAXB_DIFF_PACKAGES);
			SchemaDiff sdiff = (SchemaDiff) ser.unmarshal(reader);
			return sdiff;
		}
		catch (JAXBException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void dumpDiff(Diff diff, Writer writer) throws IOException {
		try {
			JSONSerializer ser = new JSONSerializer(JAXB_DIFF_PACKAGES);
			ser.marshal(diff, writer);
			writer.close();
		}
		catch (JAXBException e) {
			throw new RuntimeException(e);
		}
	}

}
