package main

import (
  "bufio"
  "fmt"
  "log"
  "os"
  "sort"
  "strconv"
  "strings"
  "runtime"
  "sync"
)

type StationData struct {
  count int;
  sum float64;
  min float64;
  max float64;
}

func main() {
  filePath := os.Args[1]

  fileInfo, err := os.Stat(filePath)
  if err != nil {
    log.Fatal("can not read stats of file: ", err)
  }

  wg := sync.WaitGroup{}
  cpus := runtime.NumCPU()
  defaultChunk := fileInfo.Size() / int64(cpus)
  chunkLeftovers := fileInfo.Size() % int64(cpus)

  stations := make([]map[string]StationData, cpus)

  for i := 0; i < cpus; i++ {
    currThread := i
    threadChunk := defaultChunk
    if currThread == cpus-1 {
      threadChunk += chunkLeftovers
    }

    stations[currThread] = make(map[string]StationData)

    wg.Add(1)
    go func() {
      defer wg.Done()
      file, err := os.Open(filePath)
      if err != nil {
        log.Fatal("can not open the file: ", err)
      }
      defer file.Close()

      file.Seek(int64(currThread)*defaultChunk, 0)
      lineScanner := bufio.NewScanner(file)
      if currThread > 0 {
        lineScanner.Scan() // ignore probable partial first line since it is handled by the previous chunk handler
      }

      readCount := int64(0)
      for lineScanner.Scan() && readCount <= threadChunk {
        line := lineScanner.Text()
        readCount += int64(len([]byte(line)))
        lineParts := strings.Split(line, ";")
        name := lineParts[0]
        measurement, err := strconv.ParseFloat(lineParts[1], 64)
        if err != nil {
          log.Fatalf("error parsing %s", lineParts[1])
        }

        if station, ok := stations[currThread][name]; ok {
          station.count++
          station.sum += measurement
          if measurement < station.min {
            station.min = measurement
          }

          if measurement > station.max {
            station.max = measurement
          }

          stations[currThread][name] = station

        } else {
          station := StationData{ count: 1, sum: measurement, min: measurement, max: measurement }
          stations[currThread][name] = station
        }
      }
    }()
  }

  wg.Wait()

  combinedStations := make(map[string]StationData)
  for _, currStations := range stations {
    for key := range currStations {
      currStation := currStations[key]
      if combinedStation, ok := combinedStations[key]; ok {
        combinedStation.count += currStation.count
        combinedStation.sum += currStation.sum

        if currStation.min < combinedStation.min {
          combinedStation.min = currStation.min
        }

        if currStation.max > combinedStation.max {
          combinedStation.max = currStation.max
        }

        combinedStations[key] = combinedStation
      } else {
        combinedStations[key] = currStation
      }
    }
  }

  keys := make([]string, len(combinedStations))

  i := 0
  for k := range combinedStations {
    keys[i] = k
    i++
  }

  sort.Strings(keys)

  for _, key := range keys {
    fmt.Printf("%s=%.1f/%.1f/%.1f\n", key, combinedStations[key].min, combinedStations[key].sum/float64(combinedStations[key].count), combinedStations[key].max)
  }

}
