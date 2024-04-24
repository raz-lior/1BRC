package main

import (
  "bufio"
  "fmt"
  "log"
  "os"
  "sort"
  "strconv"
  "strings"
)

type StationData struct {
  count int;
  sum float64;
  min float64;
  max float64;
}

func main() {
  filePath := os.Args[1]
  file, err := os.Open(filePath)
  if err != nil {
    log.Fatal("can not open the file: ", err)
  }
  defer file.Close()

  lineScanner := bufio.NewScanner(file)
  stations := make(map[string]StationData)

  for lineScanner.Scan() {
    line := lineScanner.Text()
    lineParts := strings.Split(line, ";")
    name := lineParts[0]
    measurement, err := strconv.ParseFloat(lineParts[1], 64)
    if err != nil {
      log.Fatalf("error parsing %s", lineParts[1])
    }

    if station, ok := stations[name]; ok {
      station.count++
      station.sum += measurement
      if measurement < station.min {
        station.min = measurement
      }

      if measurement > station.max {
        station.max = measurement
      }

      stations[name] = station

    } else {
      station := StationData{ count: 1, sum: measurement, min: measurement, max: measurement }
      stations[name] = station
    }
  }

  keys := make([]string, len(stations))

  i := 0
  for k := range stations {
    keys[i] = k
    i++
  }

  sort.Strings(keys)

  for _, key := range keys {
    fmt.Printf("%s=%.1f/%.1f/%.1f\n", key, stations[key].min, stations[key].sum/float64(stations[key].count), stations[key].max)
  }

}
