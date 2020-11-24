package main

import (
    "crypto/md5"
    "database/sql"
    "flag"
    "fmt"
    "io"
    "io/ioutil"
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
    ConnStr string
    Query   string
}

func main() {
    connStr := flag.String("conn", "", "connection string")
    queryFileName := flag.String("query", "", "query file name")

    flag.Parse()

    // Read query file
    content, err := ioutil.ReadFile(*queryFileName)
    if err != nil {
        fmt.Println(err)
        return
    }

    // Init parameters
    params := Params{ConnStr: *connStr, Query: string(content)}

    // Check and read password
    params.ConnStr, err = ReadPassword(params.ConnStr)
    if err != nil {
        fmt.Println(err)
        return
    }

    t := time.Now()
    fmt.Println("Start at: ", t.Format("2006-01-02 15:04:05"))

    UnloadTable(params)

    t = time.Now()
    fmt.Println("Finish at: ", t.Format("2006-01-02 15:04:05"))
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
        return v.Time.Format("2006-01-02 15:04:05.999999999")
    case "TIMESTAMP WITH TIME ZONE":
        return v.Time.Format("2006-01-02 15:04:05.999999999 -0700")
    case "TIMESTAMP WITH LOCAL TIME ZONE":
        return v.Time.Format("2006-01-02 15:04:05.999999999")
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

    // MD5 checksum
    go func(params Params, ciRows <- chan []string) {
        h := md5.New()
        i := 0;
        for row := range ciRows {
            io.WriteString(h, strings.Join(row, ",") + "\n")

            i++;
            if i%10000 == 0 {
                fmt.Println(i)
            }

        }
        fmt.Printf("Checksum: %x", h.Sum(nil))
        fmt.Println("")

        /*for range ciRows {
        }*/

        w.Done()
    }(params, cRows)

    fmt.Println("... Fetching rows")
    FetchRows(rows, row, columnTypes, cRows)

    rows.Close()

    fmt.Println("... Closing connection")
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

func FetchRows(rows *sql.Rows, row []interface{}, columnTypes []*sql.ColumnType, coRows chan <- []string) {
    for rows.Next() {
        if err := rows.Scan(row...); err != nil {
            fmt.Println(err)
        }

        strRow := make([]string, len(row))

        // Columns to string array
        for i, col := range row {
            typeName := columnTypes[i].DatabaseTypeName();
            strRow[i] = ToString(col, typeName)
            //fmt.Println(strRow[i])

            if typeName == "VARCHAR2" || typeName == "NVARCHAR2" || typeName == "DATE" || typeName == "TIMESTAMP" || typeName == "TIMESTAMP WITH TIME ZONE" || typeName == "TIMESTAMP WITH LOCAL TIME ZONE" {
                strRow[i] = "\"" + strings.ReplaceAll(strRow[i], "\"", "\"\"") + "\""
            }
        }

        coRows <- strRow
    }
}
