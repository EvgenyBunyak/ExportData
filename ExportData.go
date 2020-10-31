package main

import (
    "flag"
    "fmt"
    "io/ioutil"
    "os"
    "database/sql"
    _ "github.com/godror/godror"
    "strconv"
    "strings"
    "sync"
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
    tabSeparated := flag.Bool("tabSeparated", false, "Tab-separated values")

    flag.Parse()

    // Read query file
    content, err := ioutil.ReadFile(*queryFileName)
    if err != nil {
        fmt.Println(err)
        return
    }

    params := Params{ConnStr: *connStr, FileName: *fileName,
                        Query: string(content), MaxSizeMB: *maxSizeMB,
                    	DoubleQuotes: *doubleQuotes, TabSeparated: *tabSeparated}

    if params.FileName == "-" {
        params.FileName = *queryFileName
    }

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

func nullFloat64ToString(v sql.NullFloat64) string {
    if v.Valid {
        return strconv.FormatFloat(v.Float64, 'f', -1, 64)
    }
    return ""
}

func nullStringToString(v sql.NullString) string {
    if v.Valid {
        return v.String
    }
    return ""
}

func ToString(i interface{}) string {
    switch fmt.Sprintf("%T", i) {
    case "sql.NullFloat64":
        return nullFloat64ToString(i.(sql.NullFloat64))
    case "sql.NullString":
        return nullStringToString(i.(sql.NullString))
    }
    return ""
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

    // Set row
    var scannedRow = []interface{}{}
    var row = []interface{}{}

    colTypes, err := rows.ColumnTypes()
    for _, c := range colTypes {
        if c.DatabaseTypeName() == "NUMBER" {
            scannedRow = append(scannedRow, &sql.NullFloat64{0, false})
            row = append(row, sql.NullFloat64{0, false})
        } else if c.DatabaseTypeName() == "VARCHAR2" {
            scannedRow = append(scannedRow, &sql.NullString{"", false})
            row = append(row, sql.NullString{"", false})
        }
    }

    var w sync.WaitGroup
    w.Add(1)
    defer w.Wait()

    // Make channel
    cRows := make(chan []interface{})
    defer close(cRows)

    // Write to file
    go func(params Params, ciRows <- chan []interface{}) {
        writeToFile(0, params, params.MaxSizeMB, ciRows)
        w.Done()
    }(params, cRows)


    // Fetch rows
    fetchRows(rows, scannedRow, row, cRows)

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

    coError <- nil

    var w sync.WaitGroup
    w.Add(1)
    defer w.Wait()

    // Make channel
    cRows := make(chan []interface{})
    defer close(cRows)

    // Write to file
    go func(rId int, params Params, ciRows <- chan []interface{}) {
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

        // Set row
        var scannedRow = []interface{}{}
        var row = []interface{}{}

        colTypes, err := rows.ColumnTypes()
        for _, c := range colTypes {
            if c.DatabaseTypeName() == "NUMBER" {
                scannedRow = append(scannedRow, &sql.NullFloat64{0, false})
                row = append(row, sql.NullFloat64{0, false})
            } else if c.DatabaseTypeName() == "VARCHAR2" {
                scannedRow = append(scannedRow, &sql.NullString{"", false})
                row = append(row, sql.NullString{"", false})
            }
        }

        // Fetch rows
        fetchRows(rows, scannedRow, row, cRows)

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

    return db, nil
}

func fetchRows(rows *sql.Rows, scannedRow []interface{}, row []interface{}, coRows chan <- []interface{}) {
    for rows.Next() {
        if err := rows.Scan(scannedRow...); err != nil {
            fmt.Println(err)
        }

        buffer := make([]interface{}, len(row))
        copy(buffer, row)

        for i, r := range scannedRow {
            switch fmt.Sprintf("%T", r) {
                case "*sql.NullFloat64":
                    buffer[i] = *r.(*sql.NullFloat64)
                case "*sql.NullString":
                    buffer[i] = *r.(*sql.NullString)
            }
        }

        coRows <- buffer
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

func writeToFile(rId int, params Params, maxSizeMB int, ciRows <- chan []interface{}) {
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
    	// Check file size
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

        // Put row to file
        for i, col := range row {
        	val := ToString(col);

        	if params.DoubleQuotes && fmt.Sprintf("%T", col) == "sql.NullString" {
        		val = "\"" + strings.ReplaceAll(val, "\"", "\"\"") + "\""
        	}

        	if i > 0 {
        		val = sep + val
            }

            fmt.Fprint(f, val)
        }
        fmt.Fprint(f, fmt.Sprintf("\n"))
    }

    // File
    err := f.Close()
    if err != nil {
        fmt.Println("writeToFile", err)
    }
}