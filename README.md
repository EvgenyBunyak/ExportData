#### Description of parameters

##### Required parameters
###### -conn
Oracle Database connection string. If specified without a password, the password will be requested before connecting to the database
###### -query
Name of the file with the query text

##### File parameters
###### -fname
Template of the filename, without extension. If no filename is specified filename, it will be generated by the filename with query text
###### -doubleQuotes
String and date time values are enclosed within double-quote characters. Default = true
###### -tabSeparated
Use tabs as separators instead of commas. Default = true
###### -compress
After uploading to a text file, automatically compress to gzip and delete the text file. Default = false
###### -maxsize
The numeric value of the maximum size of one file in megabytes, upon reaching which the upload will continue to a new file. Default = 250

##### Multi threading parameters
The application can upload data to files in several threads. Each thread is a separate process that creates a connection to the database and uploads data for a range of values. Value ranges are created by rangeStart, rangeEnd, and batch parameters. Value ranges are created for **numeric values only**. Generated ranges of values ​​will be distributed among the threads.
For example: -rangeStart=1 -rangeEnd=100 -batch=30, the ranges will be: [1, 30], [31, 60], [61, 90], [91, 100]

###### -rangeStart
The numeric value of the beginning of the value range
###### -rangeEnd
The numeric value for the end of the value range
###### -batch
The numeric value of the size of the value range. Default = 10000
###### -parallel
Number of threads. Default = 1

#### Examples

```bash
-conn=username@localhost:1521/orcl -query=car.sql
```
```bash
-conn=username@localhost:1521/orcl -query=car.sql -fname=some_special_cars
```
```bash
-conn=username@localhost:1521/orcl -query=car.sql -doubleQuotes=false -tabSeparated=false
```
```bash
-conn=username@localhost:1521/orcl -query=car.sql -rangeStart=1 -rangeEnd=1000000 -batch=1000 -parallel=4
```
