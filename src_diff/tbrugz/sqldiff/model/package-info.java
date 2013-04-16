/*
 * see:
 * http://weblogs.java.net/blog/kohsuke/archive/2005/09/using_jaxb_20s.html
 * http://stackoverflow.com/questions/10637555/how-do-i-globally-apply-an-xmladapter-to-a-jaxb-program
 */
@XmlJavaTypeAdapters({ 
	@XmlJavaTypeAdapter(value = DBIdentifiableDiffAdapter.class, type = DBIdentifiableDiff.class),
	@XmlJavaTypeAdapter(value = TableColumnDiffAdapter.class, type = TableColumnDiff.class),
	@XmlJavaTypeAdapter(value = TableDiffAdapter.class, type = TableDiff.class)
})
package tbrugz.sqldiff.model;

import javax.xml.bind.annotation.adapters.*;
