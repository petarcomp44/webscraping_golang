package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var totalSuccess int
var total_3 int
var total_503 int
var totalUrls int
var startTime time.Time
var endTime time.Time
var tags []string
var totalTags = make(map[string]int)

type Config struct {
	InputFile        string
	OutputDir        string
	ProxyURL         string
	NumThreads       int
	Timeout          time.Duration
	Iterations       int
	MaxRetries       int
	BatchSize        int
	ProcessorPath    string
	ProcessorThreads int
	ProcessorMonitor bool
	ProcessorDir     string
	ProcessorSkip    []string
}

const (
	defaultTimeout       = 40 * time.Second
	defaultMaxConcurrent = 10
	statsInterval        = 10 * time.Second
	speedCalcWindow      = 60 * time.Second
	saveInterval         = 60 * time.Second
	errorLogFile         = "errors.txt"
	networkErrorFile     = "network_error_log.txt"
	defaultMaxRetries    = 3
	retryDelay           = 5 * time.Second
	maxRetriesFor429     = 8
)

func splitFile(inputFile string, outputDir string, urlsPerFile int) error {

	totalUrls = 0

	err := os.MkdirAll(outputDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	fileCounter := 0
	urlCounter := 0
	var outputFile *os.File

	createNewFile := func() (*os.File, error) {
		fileCounter++
		fileName := fmt.Sprintf("%s/output_file_%d.txt", outputDir, fileCounter)
		newFile, err := os.Create(fileName)
		if err != nil {
			return nil, fmt.Errorf("failed to create output file: %v", err)
		}
		// fmt.Printf("Creating new file: %s\n", fileName)
		return newFile, nil
	}

	outputFile, err = createNewFile()
	if err != nil {
		return err
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)

	for scanner.Scan() {
		retext := scanner.Text()
		if !strings.HasPrefix(retext, "https://") {
			retext = "https://www." + retext
		}
		_, err := writer.WriteString(retext + "\n")
		if err != nil {
			return fmt.Errorf("failed to write to output file: %v", err)
		}
		totalUrls++
		urlCounter++

		if urlCounter == urlsPerFile {
			writer.Flush()
			outputFile.Close()

			outputFile, err = createNewFile()
			if err != nil {
				return err
			}

			writer = bufio.NewWriter(outputFile)
			urlCounter = 0
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input file: %v", err)
	}

	writer.Flush()
	outputFile.Close()

	fmt.Println("-------------------- Successfully Finished Preparing. --------------------")
	return nil
}
func formatDuration(d time.Duration) string {
	// Format duration in HH:MM:SS
	return fmt.Sprintf("%02d:%02d:%02d", int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60)
}

func stripAnsiCodes(input string) string {
	ansiEscape := regexp.MustCompile(`\x1B\[[0-9;]*[a-zA-Z]`)
	return ansiEscape.ReplaceAllString(input, "")
}

func executeGoProgram(outputFile string, idx int, config Config) (int, error) {
	totalSteps := (totalUrls / config.BatchSize) + 1

	fmt.Printf("Processing step %d/%d (%.2f%%).\n", idx, totalSteps, (float64(idx) / float64(totalSteps) * 100))
	fmt.Printf("\n - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")

	cmdArgs := []string{"run", "source.go", "-input", outputFile, "-threads", strconv.Itoa(config.NumThreads), "-proxy", config.ProxyURL, "-timeout", config.Timeout.String(), "-max-retries", strconv.Itoa(config.MaxRetries), "-iterations", "1", "-batch-size", strconv.Itoa(config.BatchSize)}

	cmd := exec.Command("go", cmdArgs...)

	finalSuccess := 0
	final_3 := 0
	final_503 := 0
	finalProcess := 0
	finalSpeed := 0
	finalTag := make(map[string]int)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, fmt.Errorf("failed to get stdout pipe: %v", err)
	}

	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("failed to start command: %v", err)
	}

	scanner := bufio.NewScanner(stdout)

	statusRegex := regexp.MustCompile(`Status: 200: (\d+)`)
	speedRegex := regexp.MustCompile(`Speed: (\d+) URLs/m`)
	negativeThreeRegex := regexp.MustCompile(`-3: (\d+)`)
	forbiddenRegex := regexp.MustCompile(`503: (\d+)`)
	progressRegex := regexp.MustCompile(`Progress: (\d+)/(\d+)`)
	tagRegex := regexp.MustCompile(`([A-Za-z0-9_]+)\s\((\d+)\)`)

	for scanner.Scan() {
		line := scanner.Text()

		cleanLine := stripAnsiCodes(line)

		elapsedTime := time.Since(startTime)
		// fmt.Printf("%s", cleanLine)
		matches := statusRegex.FindStringSubmatch(cleanLine)
		if len(matches) > 1 {
			successCount, err := strconv.Atoi(matches[1])
			if err != nil {
				fmt.Printf("Error converting success count: %v\n", err)
				continue
			}
			finalSuccess = successCount
		}
		negativeThreeMatches := negativeThreeRegex.FindStringSubmatch(cleanLine)
		if len(negativeThreeMatches) > 1 {
			n3Count, err := strconv.Atoi(negativeThreeMatches[1])
			if err != nil {
				fmt.Printf("Error converting -3 count: %v\n", err)
				continue
			}
			final_3 = n3Count
		}

		// Check for 503
		forbiddenMatches := forbiddenRegex.FindStringSubmatch(cleanLine)
		if len(forbiddenMatches) > 1 {
			forbiddenCount, err := strconv.Atoi(forbiddenMatches[1])
			if err != nil {
				fmt.Printf("Error converting 503 count: %v\n", err)
				continue
			}
			final_503 = forbiddenCount

		}

		progressMatches := progressRegex.FindStringSubmatch(cleanLine)
		if len(progressMatches) > 2 {
			currentProgress, err := strconv.Atoi(progressMatches[1])
			if err != nil {
				fmt.Printf("Error converting current progress: %v\n", err)
				continue
			}
			// totalProgress, err := strconv.Atoi(progressMatches[2])
			if err != nil {
				fmt.Printf("Error converting total progress: %v\n", err)
				continue
			}
			finalProcess = currentProgress
			// progressTotal = totalProgress
		}
		matchesTags := tagRegex.FindAllStringSubmatch(line, -1)
		if len(matchesTags) > 1 {
			for _, match := range matchesTags {
				tag := match[1]

				count := match[2]
				countValue, err := strconv.Atoi(count)
				if err != nil {
					fmt.Printf("Error converting count: %v\n", err)
					continue
				}
				finalTag[tag] = countValue

			}
		}

		matchesSpeed := speedRegex.FindStringSubmatch(cleanLine)
		if len(matchesSpeed) > 1 {
			speed, err := strconv.Atoi(matchesSpeed[1])
			if err != nil {
				fmt.Printf("Error converting speed: %v\n", err)
				continue
			}
			finalSpeed = speed
			// fmt.Print("\033[2J\033[H")

			firstLine := fmt.Sprintf("Speed: %d URLs/m | Progress: %d/%d | 200 : %d | -3: %d | 503: %d | Time: %s | TotalSpeed: %d UPM \n", finalSpeed, finalProcess+(config.BatchSize*(idx-1)), totalUrls, totalSuccess+finalSuccess, total_3+final_3, total_503+final_503, formatDuration(elapsedTime), int(float64(finalProcess+(config.BatchSize*(idx-1)))/float64(elapsedTime.Minutes())))
			tag_output := ""
			for _, tag := range tags {
				if finalTag[tag]+totalTags[tag] > 1 {
					tag_output += fmt.Sprintf("%s (%d) | ", tag, finalTag[tag]+totalTags[tag])
				}

			}
			fmt.Printf("%s%s\n", firstLine, tag_output)
			fmt.Printf(" - - --  - - - -- - - - -- - -- --\n")
		}

	}
	if err := cmd.Wait(); err != nil {
		return 0, fmt.Errorf("failed to execute command: %v", err)
	}
	totalSuccess += finalSuccess
	total_3 += final_3
	total_503 += final_503
	for _, tag := range tags {
		totalTags[tag] = totalTags[tag] + finalTag[tag]
	}

	return totalSuccess, nil
}

func parseFlags() Config {
	config := Config{}

	flag.StringVar(&config.InputFile, "input", "", "File containing URLs to download")
	flag.StringVar(&config.OutputDir, "output", "downloaded_files", "Output directory")
	flag.StringVar(&config.ProxyURL, "proxy", "", "Proxy URL (e.g., http://user:pass@host:port)")
	flag.IntVar(&config.NumThreads, "threads", defaultMaxConcurrent, "Number of concurrent downloads")
	flag.DurationVar(&config.Timeout, "timeout", defaultTimeout, "Timeout for each request")
	flag.IntVar(&config.Iterations, "iterations", 1, "Number of iterations")
	flag.IntVar(&config.MaxRetries, "max-retries", defaultMaxRetries, "Maximum number of retries")
	flag.IntVar(&config.BatchSize, "batch-size", 22000, "Size of URL batches to process")

	flag.StringVar(&config.ProcessorPath, "processor", "", "Path to external processor")
	flag.IntVar(&config.ProcessorThreads, "pt", 4, "Processor threads")
	flag.BoolVar(&config.ProcessorMonitor, "pm", false, "Enable processor monitoring")
	flag.StringVar(&config.ProcessorDir, "pd", "", "Processor directory")

	flag.Parse()

	if config.InputFile == "" {
		fmt.Println("Please provide an input file using the -input flag")
		os.Exit(1)
	}

	return config
}

func WriteReport() {
	date := time.Now().Format("2006-01-02")
	path := filepath.Join("./reports", "Archive", date)

	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return
	}
	file, err := os.Create(filepath.Join(path, "total_report.txt"))

	if err != nil {
		return
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	defer w.Flush()

	fmt.Fprintf(w, "=== Web Analyzer Total Report ===\n\n")
	fmt.Fprintf(w, "Time Started: %s\n", startTime.Format("2006-01-02 15:04:05"))
	if !endTime.IsZero() {
		fmt.Fprintf(w, "Time Ended: %s\n", endTime.Format("2006-01-02 15:04:05"))
		fmt.Fprintf(w, "Total Duration: %s\n", endTime.Sub(startTime))
	}

}

func clearAllReports() error {
	date := time.Now().Format("2006-01-02")
	path := filepath.Join("./reports", "Archive", date)

	return os.RemoveAll(path)
}

func deleteTempFolder() error {
	// Assuming the folder name is "temp" and it's in the same directory
	tempFolder := "./temp"

	// Remove the folder and all its contents
	err := os.RemoveAll(tempFolder)
	if err != nil {
		return fmt.Errorf("failed to delete temp folder: %v", err)
	}

	return nil
}

func readTags(filename string) error {
	// Open the JSON file
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	// Read the file's content into a byte slice
	byteValue, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("error reading file: %v", err)
	}

	var result map[string]interface{}
	// Unmarshal the byte slice into the global Config struct
	err = json.Unmarshal(byteValue, &result)
	if err != nil {
		return fmt.Errorf("error unmarshaling JSON: %v", err)
	}
	for key := range result {
		tags = append(tags, key)
	}
	// fmt.Println(tags)

	return nil
}

func main() {
	inputFile := "urls.txt"
	outputDir := "temp"

	config := parseFlags()
	clearAllReports()
	deleteTempFolder()
	println(" ******* Cleared everything for best performance.")

	readTags("tags.json")

	urlsPerFile := config.BatchSize
	inputFile = config.InputFile

	startTime = time.Now()

	err := splitFile(inputFile, outputDir, urlsPerFile)
	if err != nil {
		fmt.Printf("Error during splitting: %v\n", err)
		return
	}

	WriteReport()
	idx := 0
	err = filepath.Walk(outputDir, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(filePath) == ".txt" {
			idx++
			totalSuccess, err := executeGoProgram(filePath, idx, config)
			if err != nil {
				fmt.Printf("Error during file processing: %v\n", err)
				return err
			}
			_ = totalSuccess
			fmt.Printf("\n - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")

		}
		return nil
	})
	if err != nil {
		fmt.Printf("Error during file processing: %v\n", err)
		return
	}
	// deleteTempFolder()
	// println(" ******* Started second processing")

	// date := time.Now().Format("2006-01-02")
	// inputFile = "./reports/Archive/" + date + "/statuses/-3.txt"

	// err = splitFile(inputFile, outputDir, urlsPerFile)
	// if err != nil {
	// 	fmt.Printf("Error during splitting: %v\n", err)
	// 	return
	// }

	// err = filepath.Walk(outputDir, func(filePath string, info os.FileInfo, err error) error {
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if !info.IsDir() && filepath.Ext(filePath) == ".txt" {
	// 		idx++
	// 		totalSuccess, err := executeGoProgram(filePath, idx, config)
	// 		if err != nil {
	// 			fmt.Printf("Error during file processing: %v\n", err)
	// 			return err
	// 		}
	// 		_ = totalSuccess
	// 		fmt.Printf("\n - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")

	// 	}
	// 	return nil
	// })
	// if err != nil {
	// 	fmt.Printf("Error during file processing: %v\n", err)
	// 	return
	// }
	endTime = time.Now()

	deleteTempFolder()
	fmt.Println("All files processed successfully.")
}
