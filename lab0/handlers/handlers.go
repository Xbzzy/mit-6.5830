package handlers

import (
	"encoding/base64"
	"html/template"
	rdb "main/ridership_db"
	"main/utils"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
)

func HomeHandler(w http.ResponseWriter, r *http.Request) {
	// Get the selected chart from the query parameter
	selectedChart := r.URL.Query().Get("line")
	if selectedChart == "" {
		selectedChart = "red"
	}

	// instantiate ridershipDB
	//var db rdb.RidershipDB = &rdb.SqliteRidershipDB{} // Sqlite implementation
	var db rdb.RidershipDB = &rdb.CsvRidershipDB{} // CSV implementation

	// Get the chart data from RidershipDB
	//err := db.Open("../mbta.sqlite")
	err := db.Open("../mbta.csv")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	values, err := db.GetRidership(selectedChart)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Plot the bar chart using utils.GenerateBarChart. The function will return the bar chart
	// as PNG byte slice. Convert the bytes to a base64 string, which is used to embed images in HTML.
	chartBytes, err := utils.GenerateBarChart(values)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get path to the HTML template for our web app
	_, currentFilePath, _, _ := runtime.Caller(0)
	templateFile := filepath.Join(filepath.Dir(currentFilePath), "template.html")

	// Read and parse the HTML so we can use it as our web app template
	html, err := os.ReadFile(templateFile)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmpl, err := template.New("line").Parse(string(html))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// We now want to create a struct to hold the values we want to embed in the HTML
	data := struct {
		Image string
		Chart string
	}{
		Image: base64.StdEncoding.EncodeToString(chartBytes),
		Chart: selectedChart,
	}

	// Use tmpl.Execute to generate the final HTML output and send it as a response
	// to the client's request.
	err = tmpl.Execute(w, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(200)
	return
}
