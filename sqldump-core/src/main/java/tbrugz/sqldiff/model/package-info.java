/*
 * see:
 * http://weblogs.java.net/blog/kohsuke/archive/2005/09/using_jaxb_20s.html
 * http://stackoverflow.com/questions/10637555/how-do-i-globally-apply-an-xmladapter-to-a-jaxb-program
 */
@XmlJavaTypeAdapters({ 
	@XmlJavaTypeAdapter(value = DBIdentifiableDiffAdapter.class, type = DBIdentifiableDiff.class),
	@XmlJavaTypeAdapter(value = ColumnDiffAdapter.class, type = ColumnDiff.class),
	@XmlJavaTypeAdapter(value = TableDiffAdapter.class, type = TableDiff.class),
	@XmlJavaTypeAdapter(value = GrantDiffAdapter.class, type = GrantDiff.class)
})
package tbrugz.sqldiff.model;

import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapters;
