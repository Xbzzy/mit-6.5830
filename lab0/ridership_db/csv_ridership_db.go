package ridershipDB

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
)

type CsvRidershipDB struct {
	idIdxMap      map[string]int
	csvFile       *os.File
	csvReader     *csv.Reader
	num_intervals int
}

func (c *CsvRidershipDB) Open(filePath string) error {
	c.num_intervals = 9

	// Create a map that maps MBTA's time period ids to indexes in the slice
	c.idIdxMap = make(map[string]int)
	for i := 1; i <= c.num_intervals; i++ {
		timePeriodID := fmt.Sprintf("time_period_%02d", i)
		c.idIdxMap[timePeriodID] = i - 1
	}

	// create csv reader
	csvFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	c.csvFile = csvFile
	c.csvReader = csv.NewReader(c.csvFile)

	return nil
}

// Implement the remaining RidershipDB methods

func (c *CsvRidershipDB) GetRidership(lineId string) (reply []int64, err error) {
	// line_id,direction,time_period_id,station_id,total_ons
	dataSlice, err := c.csvReader.ReadAll()
	if err != nil {
		return
	}

	var total int
	reply = make([]int64, c.num_intervals)
	for _, data := range dataSlice {
		if data[0] != lineId {
			// not the line
			continue
		}

		total, err = strconv.Atoi(data[4])
		if err != nil {
			return
		}

		reply[c.idIdxMap[data[2]]] += int64(total)
	}
	return
}

func (c *CsvRidershipDB) Close() error {
	return c.csvFile.Close()
}
