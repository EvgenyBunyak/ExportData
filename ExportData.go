package main

import (
    "database/sql"
    "flag"
    "fmt"
    "io/ioutil"
    "os"
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
    maxSizeMB := flag.Int("maxsize", 10, "file max size (MB)")

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
                        Query: string(content), MaxSizeMB: *maxSizeMB,
                    	DoubleQuotes: *doubleQuotes, TabSeparated: *tabSeparated}

    // Check and read password
    params.ConnStr, err = readPassword(params.ConnStr)

    if err != nil {
        fmt.Println(err)
        return
    }

    if params.FileName == "-" {
        params.FileName = *queryFileName
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

        runUnloadTableByRange(params, rs, re, *batchSize, *parallel)

    } else {
        unloadTable(params)
    }
}

func readPassword(connStr string) (string, error) {
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

    re, err = regexp.Compile("@")
    if err != nil {
        return "", err
    }
    newConnStr = re.ReplaceAllString(newConnStr, fmt.Sprintf("/%s@", string(bytePassword)))

    return newConnStr, nil
}

func nullFloat64ToString(v sql.NullFloat64) string {
    if !v.Valid {
        return ""
    }
    return strconv.FormatFloat(v.Float64, 'f', -1, 64)
}

func nullStringToString(v sql.NullString) string {
    if !v.Valid {
        return ""
    }
    return v.String
}

func nullTimeToString(v sql.NullTime, columnType string) string {
    if !v.Valid {
        return ""
    }

    switch columnType {
    case "DATE":
        return v.Time.Format("2006-01-02 15:04:05")
    case "TIMESTAMP":
        return v.Time.Format("2006-01-02 15:04:05.999999999")
    case "TIMESTAMP WITH TIME ZONE":
        return v.Time.Format("2006-01-02 15:04:05.999999999 -0700")
    case "TIMESTAMP WITH LOCAL TIME ZONE":
        return v.Time.Format("2006-01-02 15:04:05.999999999")
    }
    return ""
}

func toString(i interface{}, columnType string) string {
    switch fmt.Sprintf("%T", i) {
    case "*sql.NullFloat64":
        return nullFloat64ToString(*i.(*sql.NullFloat64))
    case "*sql.NullString":
        return nullStringToString(*i.(*sql.NullString))
    case "*sql.NullTime":
        return nullTimeToString(*i.(*sql.NullTime), columnType)
    }
    return ""
}

func defineColumnTypes(rows *sql.Rows) (columnTypes []*sql.ColumnType, row []interface{}, err error) {
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

func unloadTable(params Params) {
    fmt.Println("... Setting up Database Connection")
    db, err := connectToDB(params.ConnStr)
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
    columnTypes, row, err := defineColumnTypes(rows)
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
        writeToFile(0, params, params.MaxSizeMB, ciRows)
        w.Done()
    }(params, cRows)

    // Fetch rows
    fetchRows(rows, row, columnTypes, params.DoubleQuotes, cRows)

    rows.Close()

    fmt.Println("... Closing connection")
}

func runUnloadTableByRange(params Params, rangeStart int, rangeEnd int, batchSize int, parallel int) {
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
            unloadTableByRange(rId, params, ciRange, coError)
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

func unloadTableByRange(rId int, params Params, ciRange <- chan Range, coError chan <- error) {
    fmt.Println(rId, "... Setting up Database Connection")
    db, err := connectToDB(params.ConnStr)
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
        writeToFile(rId, params, params.MaxSizeMB, ciRows)
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
	    columnTypes, row, err := defineColumnTypes(rows)
	    if err != nil {
	    	fmt.Println(rId, "... Error defining column types", err)
	        return
	    }

        // Fetch rows
        fetchRows(rows, row, columnTypes, params.DoubleQuotes, cRows)

        rows.Close()
    }

    fmt.Println(rId, "... Closing connection")
}

func connectToDB(connStr string) (db *sql.DB, err error) {
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

func fetchRows(rows *sql.Rows, row []interface{}, columnTypes []*sql.ColumnType, doubleQuotes bool, coRows chan <- []string) {
    for rows.Next() {
        if err := rows.Scan(row...); err != nil {
            fmt.Println(err)
        }

        strRow := make([]string, len(row))

        // Columns to string array
        for i, col := range row {
            typeName := columnTypes[i].DatabaseTypeName();
            strRow[i] = toString(col, typeName)

            if doubleQuotes && (typeName == "VARCHAR2" || typeName == "NVARCHAR2" || typeName == "DATE" || typeName == "TIMESTAMP" || typeName == "TIMESTAMP WITH TIME ZONE" || typeName == "TIMESTAMP WITH LOCAL TIME ZONE") {
                strRow[i] = "\"" + strings.ReplaceAll(strRow[i], "\"", "\"\"") + "\""
            }
        }

        coRows <- strRow
    }
}

func newFile(fileName string, format string, rId int, counter int) (*os.File, int) {
    c := counter;
    c++;

    fn := fileName;
    if rId == 0 {
        fn += fmt.Sprintf("_%07d." + format, c);
    } else {
        fn += fmt.Sprintf("_%d_%07d." + format, rId, c)
    }

    f, err := os.Create(fn)
    if err != nil {
        fmt.Println("newFile", err)
        f.Close()
    }
    return f, c
}

func getSizeMB(f *os.File) float64 {
    fi, err := f.Stat()
    if err != nil {
        fmt.Println("getSizeMB", err)
    }
    return float64(fi.Size()) / 1024 / 1024
}

func writeToFile(rId int, params Params, maxSizeMB int, ciRows <- chan []string) {
	counter := 0;

	sep := ",";
    format := "csv";
    if params.TabSeparated {
    	sep = "\t"
    	format = "tsv"
    }

    f, counter := newFile(params.FileName, format, rId, counter)

    i := 0;
    for row := range ciRows {
    	// Check file size one time per N rows
        i++;
        if i >= 1000 {
            i = 0;
            if float64(maxSizeMB) * float64(0.95) <= getSizeMB(f) {
                err := f.Close()
                if err != nil {
                    fmt.Println("writeToFile", err)
                }
                f, counter = newFile(params.FileName, format, rId, counter)
            }
        }

        fmt.Fprintln(f, strings.Join(row, sep))
    }

    // File
    err := f.Close()
    if err != nil {
        fmt.Println("writeToFile", err)
    }
}