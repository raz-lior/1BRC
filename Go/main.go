package main

import (
  "fmt"
  "io"
  "log"
  "os"
  "runtime"
  "sort"
  "strconv"
  "sync"
  "strings"
  "unsafe"
)

const bufSize = 10*1024
const smallBufSize = 100
const dash = byte('-')
const period = byte('.')
const newline = byte('\n')
const semicolon = byte(';')

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

  stations := make([]map[string]*StationData, cpus)

  for i := 0; i < cpus; i++ {
    currThread := i
    threadChunk := defaultChunk
    if currThread == cpus-1 {
      threadChunk += chunkLeftovers
    }

    stations[currThread] = make(map[string]*StationData)

    wg.Add(1)
    go func() {
      defer wg.Done()
      file, err := os.Open(filePath)
      if err != nil {
        log.Fatalln("can not open the file: ", err)
      }
      defer file.Close()

      buf := make([]byte, bufSize+smallBufSize)
      smallBuf := make([]byte, smallBufSize)

      file.Seek(int64(currThread)*defaultChunk, 0)
      if currThread > 0 {
        // ignore probable partial first line since it is handled by the previous chunk handler
        readLine(file, &smallBuf)
      }

      readCount := int64(0)
      for readCount <= threadChunk {
        clear(buf)
        clear(smallBuf)
        buf = buf[:bufSize]

        n, err := file.Read(buf)
        if err == io.EOF {
          return
        } else if err != nil {
          log.Fatalln("got error while reading the file: ", err)
        }

        buf = buf[:n]

        // read the rest of the line and add to buffer
        if buf[len(buf)-1] != newline {

          restOfLine := readLine(file, &smallBuf)

          if len(restOfLine) > 0 {
            buf = append(buf, restOfLine...)
          }
        }

        prvLineIdx := -1
        prvSemiIdx := -1
        name := ""
        measurement := 0.0
        for i := 0; i < len(buf); i++ {

          if buf[i] == newline {
            measurementBuf := buf[prvSemiIdx+1:i]
            measurement, err = strconv.ParseFloat(*(*string)(unsafe.Pointer(&measurementBuf)), 64)
            prvLineIdx = i
          } else if buf[i] == semicolon {
            nameBuf := buf[prvLineIdx+1:i]
            name = *(*string)(unsafe.Pointer(&nameBuf))
            prvSemiIdx = i
            continue
          } else {
            continue
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

          } else {
            station := &StationData{ count: 1, sum: measurement, min: measurement, max: measurement }
            stations[currThread][strings.Clone(name)] = station
          }

          if readCount + int64(i) + 1 >= threadChunk {
            break
          }
        }

        readCount += int64(len(buf))
      }
    }()
  }

  wg.Wait()

  combinedStations := make(map[string]*StationData)
  for i := 0; i < len(stations); i++ {
    currStations := stations[i]
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

func readLine(file *os.File, buf *[]byte) []byte {
  m, err := file.Read(*buf)
  if err != nil {
    log.Fatalln("can not read initial part. ", err)
  }

  newLineIdx := -1
  for i := 0; i < m; i++ {
    if (*buf)[i] == newline {
      newLineIdx = i
      break
    }
  }

  if m > 0 {
    file.Seek(int64(newLineIdx+1-m),1)
  }

  return (*buf)[:newLineIdx+1]
}
