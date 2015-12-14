// Implement filtering a list of file names with a pipeline
// of go routines and channels.
//
// Example:
// ./filter-pipeline -max 2000 -suffixes .pdf,.txt ~/Documents/*

package main

import (
  "flag"
  "fmt"
  "log"
  "os"
  "path/filepath"
  "runtime"
  "strings"
)

func main() {
  runtime.GOMAXPROCS(runtime.NumCPU()) // use all the machine's cores
  log.SetFlags(0)
  minSize, maxSize, suffixes, files := handleCommandLine()
  sink(
    filterSize(minSize, maxSize,
      filterSuffixes(suffixes,
        source(files))))
}

func handleCommandLine() (minSize, maxSize int64, suffixes, files []string) {
  flag.Int64Var(&minSize, "min", -1, "minimum file size (-1 means no minimum)")
  flag.Int64Var(&maxSize, "max", -1, "maximum file size (-1 means no maximum)")
  var suffixesOpt *string = flag.String("suffixes", "", "comma-separated list of file suffixes")

  flag.Parse()

  if minSize > maxSize && maxSize != -1 {
    log.Fatalln("minimum size must be < maximum size")
  }

  suffixes = []string{}
  if *suffixesOpt != "" {
    suffixes = strings.Split(*suffixesOpt, ",")
  }

  files = flag.Args()

  return minSize, maxSize, suffixes, files
}

func source(files []string) <-chan string {
  out := make(chan string, 1000)
  go func() {
    for _, filename := range files {
      out <- filename
    }
    close(out)
  }()
  return out
}

func filterSuffixes(suffixes []string, in <-chan string) <-chan string {
  out := make(chan string, cap(in))
  go func() {
    if len(suffixes) == 0 {
      for filename := range in {
        out <- filename
      }
    } else {
      for filename := range in {
        ext := strings.ToLower(filepath.Ext(filename))
        for _, suffix := range suffixes {
          if ext == suffix {
            out <- filename
            break
          }
        }
      }
    }
    close(out)
  }()
  return out
}

func filterSize(minSize, maxSize int64, in <-chan string) <-chan string {
  out := make(chan string, cap(in))
  go func() {
    if minSize == -1 && maxSize == -1 {
      for filename := range in {
        out <- filename
      }
    } else {
      for filename := range in {
        finfo, err := os.Stat(filename)
        if err != nil {
          continue
        }
        size := finfo.Size()
        if (minSize == -1 || -1 < minSize && minSize < size) &&
           (maxSize == -1 || -1 < maxSize && size < maxSize) {
          out <- filename
        }
      }
    }
    close(out)
  }()
  return out  
}

func sink(in <-chan string) {
  for filename := range in {
    fmt.Println(filename)
  }
}