rem when compiled from source
java -cp "bin/main;lib/*;<jdbc drivers>" tbrugz.sqldump.SQLDump -propfile=<propfile>

rem when jar built from source
java -cp "dist/jars/sqldump.jar;lib/*:<jdbc drivers>" tbrugz.sqldump.SQLDump -propfile=<propfile>

rem when downloaded sqldump.jar (& dependencies - into "lib")
java -cp "sqldump.jar;lib/*;<jdbc drivers>" tbrugz.sqldump.SQLDump -propfile=<propfile>

pause
