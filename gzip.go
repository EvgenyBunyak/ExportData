package main

import (
    "bufio"
    "compress/gzip"
    "fmt"
    "io"
    "io/ioutil"
    "os"
    "time"
)

func main() {
    fileName := "small.txt";
    CreateTestFile(fileName)
    CompressFile(fileName)
}

func CompressFile(fileName string) {
    fmt.Println("Start at: ", time.Now().Format("2006-01-02 15:04:05"))

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

    fmt.Println("Finish at: ", time.Now().Format("2006-01-02 15:04:05"))
}

func CompressFileOld(fileName string) {
    t := time.Now()
    fmt.Println("Start at: ", t.Format("2006-01-02 15:04:05"))

    f, _ := os.Open(fileName)

    gz, err := os.Create(fileName + ".gz")
    if err != nil {
        fmt.Println(err)
        gz.Close()
    }

    reader := bufio.NewReader(f)
    content, _ := ioutil.ReadAll(reader)

    w := gzip.NewWriter(gz)
    w.Write(content)
    w.Close()

    err = gz.Close()
    if err != nil {
        fmt.Println("CompressFile", err)
    }

    err = f.Close()
    if err != nil {
        fmt.Println("CompressFile", err)
    }

    t = time.Now()
    fmt.Println("Finish at: ", t.Format("2006-01-02 15:04:05"))
}

func CloseFile(f *os.File, compress bool) {
    fileName := f.Name();

    err := f.Close()
    if err != nil {
        fmt.Println("CloseFile", err)
    }

    CompressFile(fileName)
}

func CreateTestFile(fileName string) {
    f, err := os.Create(fileName)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer f.Close()

    for i := 0; i < 500; i++ {
        fmt.Fprintln(f, "1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890")
    }
}