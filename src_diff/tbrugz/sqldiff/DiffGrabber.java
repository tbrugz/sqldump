package tbrugz.sqldiff;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Reader;

import javax.xml.bind.JAXBException;

import tbrugz.sqldiff.model.Diff;

public interface DiffGrabber {
	String type();
	Diff grabDiff(Reader reader) throws JAXBException;
	Diff grabDiff(File file) throws JAXBException, FileNotFoundException;
}
