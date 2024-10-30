package main

import (
	"fmt"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

func main() {
	// Create a new application
	app := tview.NewApplication()

	// Title
	title := tview.NewTextView().
		SetText("[yellow::b]System Monitoring UI[white]\n").
		SetDynamicColors(true).
		SetTextAlign(tview.AlignCenter)

	// Create a table for displaying stats (e.g., CPU, Memory usage)
	table := tview.NewTable().
		SetBorders(true).
		SetFixed(1, 1) // Fix the header row and first column

	// Add header row
	table.SetCell(0, 0, tview.NewTableCell("[::b]Metric")).
		SetCell(0, 1, tview.NewTableCell("[::b]Value"))

	// Set initial values for metrics
	updateTableValues(table)

	// Create a flex layout to display title and table
	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(title, 3, 1, false). // Title at the top (3 rows)
		AddItem(table, 0, 1, true)   // Table takes remaining space

	// Update the table values every second to simulate real-time data
	go func() {
		for {
			app.QueueUpdateDraw(func() {
				updateTableValues(table)
			})
			time.Sleep(1 * time.Second)
		}
	}()

	// Fixed key bindings: 'q' to quit
	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyRune && event.Rune() == 'q' {
			app.Stop() // Stop app on 'q'
		}
		return event
	})

	// Set the root and run the application
	if err := app.SetRoot(flex, true).Run(); err != nil {
		panic(err)
	}
}

// Function to update the table values
func updateTableValues(table *tview.Table) {
	// Update the metric values (example: CPU and Memory usage)
	table.SetCell(1, 0, tview.NewTableCell("CPU Usage")).
		SetCell(1, 1, tview.NewTableCell(fmt.Sprintf("%.2f%%", getCPUUsage())))

	table.SetCell(2, 0, tview.NewTableCell("Memory Usage")).
		SetCell(2, 1, tview.NewTableCell(fmt.Sprintf("%.2fMB", getMemoryUsage())))
}

// Simulated CPU Usage (random value for example)
func getCPUUsage() float64 {
	return 20.0 + 10.0*float64(time.Now().Second()%10)
}

// Simulated Memory Usage (random value for example)
func getMemoryUsage() float64 {
	return 1024.0 + 512.0*float64(time.Now().Second()%5)
}
