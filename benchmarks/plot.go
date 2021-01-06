package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

// Plots throughput configuration of multiple binaries in a file
// Conditions:
// One file should hold throughputs of multiple binaries for a given common configuration (PlotData.configuration)
// - Each binary can output multiple thoughputs. E.g parser outputs read and write throughput on one binary
// - Binary name is identified with PlotData.id. All the binary names form X axis


func main() {
	files, err := ioutil.ReadDir("results")
	if err != nil {
		log.Fatal(err)
	}

	// results
	f, _ := os.Create("results/benchmarks.html")

	for _, file := range files {
		fileName := "results/" + file.Name()
		if filepath.Ext(fileName) == ".txt" {
			// fmt.Println(fileName)
			data, err := ioutil.ReadFile(fileName) // just pass the file name
			check(err)
			plotsData := extract(string(data))
			plot(fileName, f, plotsData)
		}
	}
}

// Extract multiple benchmark configuration in a file
func extract(data string) []*PlotData {
	// fmt.Println(data)
	o := make([]*PlotData, 0)
	v := strings.SplitAfter(data, "}")

	// Iterate through multiple benchmarks in a file
	for _, info := range v {
		// Ignore last empty string
		if strings.TrimSpace(info) == "" {
			continue
		}

		d := make(map[string]interface{})
		err := json.Unmarshal([]byte(info), &d)
		check(err)

		// Separate throughput and xaxis configuration from a benchmark
		plotData := NewPlotData()
		for k, v := range d {
			// fmt.Println(k, v)

			// Collect ids, throughputs and common configuration across all binaries
			if k == "id" {
				switch i := v.(type) {
				case string:
					plotData.addID(i)
				default:
					log.Fatalln("getFloat: unknown value is of incompatible type = ", i)
				}
			} else if strings.Contains(k, "throughput") {
				switch i := v.(type) {
				case float64:
					plotData.addThroughput(k, i)
				default:
					log.Fatalln("getFloat: unknown value is of incompatible type = ", i)
				}
			} else {
				switch i := v.(type) {
				case float64:
					value := fmt.Sprintf("%.2f", i)
					plotData.addInformation(k + " = " + value)
				case string:
					plotData.addInformation(k + " = " + i)
				default:
					log.Fatalln("getFloat: unknown value is of incompatible type = ", i)
				}
			}
		}

		o = append(o, plotData)
	}

	return o
}

// Plot throughputs of different binaries in a single file
func plot(name string, file *os.File, plotsData []*PlotData) {
	bar := charts.NewBar()

	var x []string
	var throughputs map[string][]opts.BarData = make(map[string][]opts.BarData)
	var configuration = ""
	// var legend = []string{}
	for _, plotData := range plotsData {
		// Collect x axis and configuration. Configuration is overwritten
		// as it's common across all binaries in file
		x = append(x, plotData.id)
		configuration = plotData.configuration

		// If each binary produces multiple throughputs, like
		// read and write throughtputs. Collect all read throughputs
		// first and then colllect all write throughputs in an array
		for k, v := range plotData.throughputs {
			throughputs[k] = append(throughputs[k], opts.BarData{Value: v})
		}
	}

	bar.SetGlobalOptions(charts.WithTitleOpts(opts.Title{
		Title: name,
		Subtitle: configuration,
	}),
	charts.WithLegendOpts(opts.Legend{Show: true}),
	charts.WithTooltipOpts(opts.Tooltip{Show: true}),
	charts.WithYAxisOpts( opts.YAxis{Data: true}),
	)

	bar.SetXAxis(x)
	for k, v := range throughputs {
		bar.AddSeries(k, v,  charts.WithBarChartOpts(opts.BarChart{BarGap: "0"}))
	}

	bar = bar.XYReversal()
	bar.Render(file)
}

func check(e error) {
	if e != nil {
		log.Fatal(e)
		os.Exit(1)
	}
}

type PlotData struct {
	id            string
	throughputs   map[string]float64
	configuration string
}

func NewPlotData() *PlotData {
	return &PlotData{ throughputs: map[string]float64{}, configuration: ""}
}

func(p *PlotData) addID(id string) {
	p.id = id
}

func(p *PlotData) addInformation(info string) {
	if p.configuration == "" {
		p.configuration = info
		return
	}

	p.configuration = p.configuration + ", " + info
}

func(p *PlotData) addThroughput(info string, throughput float64) {
	p.throughputs[info] = throughput
}