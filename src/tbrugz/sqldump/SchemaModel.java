package tbrugz.sqldump;

import java.util.HashSet;
import java.util.Set;

public class SchemaModel {
	Set<Table> tables = new HashSet<Table>();
	Set<FK> foreignKeys = new HashSet<FK>();
}
