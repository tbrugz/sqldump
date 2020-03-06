# when compiled from source
java -cp "bin/main:lib/*:<jdbc drivers>" tbrugz.sqldump.SQLDump -propfile=<propfile>

# when jar built from source
java -cp "dist/jars/sqldump.jar:lib/*:<jdbc drivers>" tbrugz.sqldump.SQLDump -propfile=<propfile>

# when downloaded sqldump.jar (& dependencies - into "lib")
java -cp "sqldump.jar:lib/*:<jdbc drivers>" tbrugz.sqldump.SQLDump -propfile=<propfile>
