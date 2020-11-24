package main

import (
    "bufio"
    "compress/gzip"
    "database/sql"
    "flag"
    "fmt"
    "io"
    "io/ioutil"
    "os"
    "path/filepath"
    "regexp"
    "strconv"
    "strings"
    "sync"
    "syscall"
    "time"

    "golang.org/x/crypto/ssh/terminal"
    _ "github.com/godror/godror"
)

type Params struct {
    ConnStr             string
    FileName        	string
    Query           	string
    MaxSizeMB       	int
    Compress            bool
    DoubleQuotes 		bool
    TabSeparated 		bool
}

type Range struct {
    FirstValue  int
    LastValue   int
}

func main() {
    connStr := flag.String("conn", "", "connection string")
    queryFileName := flag.String("query", "", "query file name")
    fileName := flag.String("fname", "-", "file name, without extension")
    maxSizeMB := flag.Int("maxsize", 250, "file max size (MB)")
    compress := flag.Bool("compress", false, "compress to gzip file")

    parallel := flag.Int("parallel", 1, "parallel level")
    rangeStart := flag.String("rangeStart", "-", "range start value")
    rangeEnd := flag.String("rangeEnd", "-", "range end value")
    batchSize := flag.Int("batch", 10000, "batch size (row count)")

    doubleQuotes := flag.Bool("doubleQuotes", true, "Double quotes")
    tabSeparated := flag.Bool("tabSeparated", true, "Tab-separated values")

    flag.Parse()

    // Read query file
    content, err := ioutil.ReadFile(*queryFileName)
    if err != nil {
        fmt.Println(err)
        return
    }

    // Init parameters
    params := Params{ConnStr: *connStr, FileName: *fileName,
                        Query: string(content), MaxSizeMB: *maxSizeMB, Compress: *compress,
                    	DoubleQuotes: *doubleQuotes, TabSeparated: *tabSeparated}

    // Check and read password
    params.ConnStr, err = ReadPassword(params.ConnStr)

    if err != nil {
        fmt.Println(err)
        return
    }

    // Generate file name
    if params.FileName == "-" {
        params.FileName = TrimExtension(*queryFileName)
    }

    // Read range
    if *rangeStart != "-" && *rangeEnd != "-" {
        rs, err := strconv.Atoi(*rangeStart)
        if err != nil {
            fmt.Println(err)
            return
        }
        re, err := strconv.Atoi(*rangeEnd)
        if err != nil {
            fmt.Println(err)
            return
        }

        RunUnloadTableByRange(params, rs, re, *batchSize, *parallel)

    } else {
        UnloadTable(params)
    }
}

func ReadPassword(connStr string) (string, error) {
    re, err := regexp.Compile(".+/{1}.+@{1}")
    if err != nil {
        return "", err
    }

    if re.MatchString(connStr) {
        return connStr, nil
    }

    newConnStr := connStr;

    fmt.Print("Enter password: ")
    bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
    if err != nil {
        return "", err
    }

    fmt.Println("")

    re, err = regexp.Compile("@")
    if err != nil {
        return "", err
    }
    newConnStr = re.ReplaceAllString(newConnStr, fmt.Sprintf("/%s@", string(bytePassword)))

    return newConnStr, nil
}

func NullFloat64ToString(v sql.NullFloat64) string {
    if !v.Valid {
        return ""
    }
    return strconv.FormatFloat(v.Float64, 'f', -1, 64)
}

func NullStringToString(v sql.NullString) string {
    if !v.Valid {
        return ""
    }
    return v.String
}

func NullTimeToString(v sql.NullTime, columnType string) string {
    if !v.Valid {
        return ""
    }

    switch columnType {
    case "DATE":
        return v.Time.Format("2006-01-02 15:04:05")
    case "TIMESTAMP":
        return v.Time.Format("2006-01-02 15:04:05.000000000")
    case "TIMESTAMP WITH TIME ZONE":
        return v.Time.Format("2006-01-02 15:04:05.000000000 -0700")
    case "TIMESTAMP WITH LOCAL TIME ZONE":
        return v.Time.Format("2006-01-02 15:04:05.000000000")
    }
    return ""
}

func ToString(i interface{}, columnType string) string {
    switch fmt.Sprintf("%T", i) {
    case "*sql.NullFloat64":
        return NullFloat64ToString(*i.(*sql.NullFloat64))
    case "*sql.NullString":
        return NullStringToString(*i.(*sql.NullString))
    case "*sql.NullTime":
        return NullTimeToString(*i.(*sql.NullTime), columnType)
    }
    return ""
}

func DefineColumnTypes(rows *sql.Rows) (columnTypes []*sql.ColumnType, row []interface{}, err error) {
    columnTypes, err = rows.ColumnTypes()
    if err != nil {
    	return
    }

    for _, c := range columnTypes {
        if c.DatabaseTypeName() == "NUMBER" {
            //row = append(row, &sql.NullFloat64{0, false})
            row = append(row, &sql.NullString{"", false})
        } else if c.DatabaseTypeName() == "VARCHAR2" || c.DatabaseTypeName() == "NVARCHAR2" {
            row = append(row, &sql.NullString{"", false})
        } else if c.DatabaseTypeName() == "DATE" || c.DatabaseTypeName() == "TIMESTAMP" || c.DatabaseTypeName() == "TIMESTAMP WITH TIME ZONE" || c.DatabaseTypeName() == "TIMESTAMP WITH LOCAL TIME ZONE" {
            row = append(row, &sql.NullTime {time.Time{}, false})
        } else {
        	err = fmt.Errorf("Unexpected type: %s ", c.DatabaseTypeName())
        }
    }

    return
}

func UnloadTable(params Params) {
    fmt.Println("... Setting up Database Connection")
    db, err := ConnectToDB(params.ConnStr)
    if err != nil {
        fmt.Println("... DB Setup Failed")
        fmt.Println(err)
        return
    }
    defer db.Close()

    // Exec query
    rows, err := db.Query(params.Query)
    if err != nil {
        fmt.Println("... Error processing query")
        fmt.Println(err)
        return
    }

    // Define column types
    columnTypes, row, err := DefineColumnTypes(rows)
    if err != nil {
        fmt.Println("... Error defining column types")
        fmt.Println(err)
        return
    }

    var w sync.WaitGroup
    w.Add(1)
    defer w.Wait()

    // Make channel
    cRows := make(chan []string)
    defer close(cRows)

    // Write to file
    go func(params Params, ciRows <- chan []string) {
        WriteToFile(0, params, params.MaxSizeMB, ciRows)
        w.Done()
    }(params, cRows)

    // Fetch rows
    FetchRows(rows, row, columnTypes, params.DoubleQuotes, cRows)

    rows.Close()

    fmt.Println("... Closing connection")
}

func RunUnloadTableByRange(params Params, rangeStart int, rangeEnd int, batchSize int, parallel int) {
    var w sync.WaitGroup
    w.Add(parallel)
    defer w.Wait()

    // Make error channel
    cError := make(chan error)
    defer close(cError)

    // Make range channel
    cRange := make(chan Range)
    defer close(cRange)

    for p := 1; p <= parallel; p++ {
        go func(rId int, params Params, ciRange <- chan Range, coError chan <- error) {
            UnloadTableByRange(rId, params, ciRange, coError)
            w.Done()
        }(p, params, cRange, cError)
    }

    // Check errors
    errors := []error{}
    for p := 1; p <= parallel; p++ {
        err := <- cError
        if err != nil {
            errors = append(errors, err)
        }
    }
    if len(errors) > 0 {
        for _, err := range errors {
            fmt.Println(err)
        }
        return
    }

    // Generation of ranges
    for f := rangeStart; f <= rangeEnd; f += batchSize {
        l := f + batchSize - 1;
        if l > rangeEnd {
            l = rangeEnd;
        }
        cRange <- Range{f, l}
    }
}

func UnloadTableByRange(rId int, params Params, ciRange <- chan Range, coError chan <- error) {
    fmt.Println(rId, "... Setting up Database Connection")
    db, err := ConnectToDB(params.ConnStr)
    if err != nil {
        coError <- err
        return
    }
    defer db.Close()

    // Error - Ok
    coError <- nil

    var w sync.WaitGroup
    w.Add(1)
    defer w.Wait()

    // Make channel
    cRows := make(chan []string)
    defer close(cRows)

    // Write to file
    go func(rId int, params Params, ciRows <- chan []string) {
        WriteToFile(rId, params, params.MaxSizeMB, ciRows)
        w.Done()
    }(rId, params, cRows)

    // Get range
    for r := range ciRange {
        fmt.Println(rId, "range", r.FirstValue, r.LastValue)

        // Exec query
        rows, err := db.Query(params.Query, r.FirstValue, r.LastValue)
        if err != nil {
            fmt.Println(rId, "... Error processing query", err)
            return
        }

        // Define column types
	    columnTypes, row, err := DefineColumnTypes(rows)
	    if err != nil {
	    	fmt.Println(rId, "... Error defining column types", err)
	        return
	    }

        // Fetch rows
        FetchRows(rows, row, columnTypes, params.DoubleQuotes, cRows)

        rows.Close()
    }

    fmt.Println(rId, "... Closing connection")
}

func ConnectToDB(connStr string) (db *sql.DB, err error) {
    // Connect
    db, err = sql.Open("godror", connStr)
    if err != nil {
        return nil, err
    }

    if err = db.Ping(); err != nil {
        return nil, err
    }

    if _, err = db.Exec("alter session set time_zone='UTC'"); err != nil {
        return nil, err
    }

    if _, err = db.Exec("alter session set NLS_NUMERIC_CHARACTERS = '. '"); err != nil {
        return nil, err
    }

    return db, nil
}

func FetchRows(rows *sql.Rows, row []interface{}, columnTypes []*sql.ColumnType, doubleQuotes bool, coRows chan <- []string) {
    for rows.Next() {
        if err := rows.Scan(row...); err != nil {
            fmt.Println(err)
        }

        strRow := make([]string, len(row))

        // Columns to string array
        for i, col := range row {
            typeName := columnTypes[i].DatabaseTypeName();
            strRow[i] = ToString(col, typeName)

            if doubleQuotes && (typeName == "VARCHAR2" || typeName == "NVARCHAR2" || typeName == "DATE" || typeName == "TIMESTAMP" || typeName == "TIMESTAMP WITH TIME ZONE" || typeName == "TIMESTAMP WITH LOCAL TIME ZONE") {
                strRow[i] = "\"" + strings.ReplaceAll(strRow[i], "\"", "\"\"") + "\""
            }
        }

        coRows <- strRow
    }
}

func TrimExtension(fileName string) string {
    extension := filepath.Ext(fileName)
    name := fileName[0:len(fileName)-len(extension)]
    return name
}

func NewFile(fileName string, extension string, rId int, counter *int) (*os.File) {
    *counter++;

    fn := fileName;
    if rId == 0 {
        fn += fmt.Sprintf("_%07d." + extension, *counter);
    } else {
        fn += fmt.Sprintf("_%d_%07d." + extension, rId, *counter)
    }

    f, err := os.Create(fn)
    if err != nil {
        fmt.Println("NewFile", err)
        f.Close()
    }
    return f
}

func CompressFile(fileName string) {
    file, err := os.Open(fileName)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer file.Close()

    gz, err := os.Create(fileName + ".gz")
    if err != nil {
        fmt.Println(err)
        return
    }
    defer gz.Close()

    w := gzip.NewWriter(gz)
    defer w.Close()

    reader := bufio.NewReader(file)
    buf := make([]byte, 1024)

    for {
        n, err := reader.Read(buf)

        if err != nil {
            if err != io.EOF {
                fmt.Println(err)
            }
            break
        }

        w.Write(buf[0:n])
    }
}

func CloseFile(f *os.File, compress bool) {
    fileName := f.Name();

    err := f.Close()
    if err != nil {
        fmt.Println("CloseFile", err)
    }

    if !compress {
        return
    }

    CompressFile(fileName)

    err = os.Remove(fileName) 
    if err != nil { 
        fmt.Println("CloseFile", err)
    }
}

func GetSizeMB(f *os.File) float64 {
    fi, err := f.Stat()
    if err != nil {
        fmt.Println("GetSizeMB", err)
    }
    return float64(fi.Size()) / 1024 / 1024
}

func WriteToFile(rId int, params Params, maxSizeMB int, ciRows <- chan []string) {
	counter := 0;

	sep := ",";
    extension := "csv";
    if params.TabSeparated {
    	sep = "\t"
    	extension = "tsv"
    }

    f := NewFile(params.FileName, extension, rId, &counter)

    i := 0;
    for row := range ciRows {
    	// Check file size one time per N rows
        i++;
        if i >= 1000 {
            i = 0;
            compressionRatio := 1.0;
            if params.Compress {
                compressionRatio = 0.11;
            }
            if float64(maxSizeMB) * float64(0.95) <= GetSizeMB(f) * compressionRatio {
                CloseFile(f, params.Compress)
                f = NewFile(params.FileName, extension, rId, &counter)
            }
        }

        fmt.Fprintln(f, strings.Join(row, sep))
    }

    CloseFile(f, params.Compress)
}