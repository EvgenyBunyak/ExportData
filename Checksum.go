package main

import (
    "crypto/md5"
    "fmt"
    "io"
    "time"
)

func main() {
    t := time.Now()
    fmt.Println("Start at: ", t.Format("2006-01-02 15:04:05"))

    h := md5.New()
    /*for i := 0; i <= 100000000; i++ {
        io.WriteString(h, "62598301 0 2020-04-01 14:55:56.000041962 2020-07-15 19:02:34.599153 41962120 1210 2020-04-16 14:55:56 24 52 9 2 1215 0 400129376120 3 1 1\n")
    }*/
    //s := "Hello, World!";
    s := "\u0754";
    io.WriteString(h, s)

    fmt.Printf("Bytes: %d Checksum: %x\n", len(s), h.Sum(nil))

    t = time.Now()
    fmt.Println("Finish at: ", t.Format("2006-01-02 15:04:05"))
}