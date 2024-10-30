package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

var totalSuccess int
var total_3 int
var total_503 int
var totalUrls int
var startTime time.Time

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

		_, err := writer.WriteString(scanner.Text() + "\n")
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

func executeGoProgram(outputFile string, idx int) (int, error) {
	totalSteps := (totalUrls / 22000) + 1

	fmt.Printf("Processing step %d/%d (%.2f%%).\n", idx, totalSteps, (float64(idx) / float64(totalSteps) * 100))
	fmt.Printf("\n - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")

	cmdArgs := []string{"run", "source.go", "-input", outputFile, "-threads", "2000", "-proxy", "http://user-spmnrwxyie-country-us:Fe7zDAf1Hxrkoj8_3m@isp.smartproxy.com:10000", "-timeout", "20s", "-max-retries", "2", "-iterations", "1", "-batch-size", "22000"}
	cmd := exec.Command("go", cmdArgs...)

	finalSuccess := 0
	final_3 := 0
	final_503 := 0
	finalProcess := 0
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

		// Check for Speed
		matchesSpeed := speedRegex.FindStringSubmatch(cleanLine)
		if len(matchesSpeed) > 1 {
			speed, err := strconv.Atoi(matchesSpeed[1])
			if err != nil {
				fmt.Printf("Error converting speed: %v\n", err)
				continue
			}
			// fmt.Printf("\r+-------------------------------------------------------------------------------------------------------------------+\n")
			fmt.Printf("\rSpeed: %d URLs/m | Progress: %d/%d | 200 : %d | -3: %d | 503: %d | ElapsedTime: %s", speed, finalProcess+(22000*(idx-1)), totalUrls, totalSuccess+finalSuccess, total_3+final_3, total_503+final_503, formatDuration(elapsedTime))
			// fmt.Printf("\r+-------------------------------------------------------------------------------------------------------------------+\n")
		}

	}
	if err := cmd.Wait(); err != nil {
		return 0, fmt.Errorf("failed to execute command: %v", err)
	}
	totalSuccess += finalSuccess
	total_3 += final_3
	total_503 += final_503
	return totalSuccess, nil
}

func main() {
	inputFile := "urls.txt"
	outputDir := "output_files"
	urlsPerFile := 22000

	startTime = time.Now()
	err := splitFile(inputFile, outputDir, urlsPerFile)
	if err != nil {
		fmt.Printf("Error during splitting: %v\n", err)
		return
	}

	idx := 0
	err = filepath.Walk(outputDir, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(filePath) == ".txt" {
			idx++
			totalSuccess, err := executeGoProgram(filePath, idx)
			if err != nil {
				fmt.Printf("Error during file processing: %v\n", err)
				return err
			}
			_ = totalSuccess
			// Display the result of the program execution
			// fmt.Printf("\nResult for file %s:\n%s\n", filePath, totalSuccess)
			fmt.Printf("\n - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")

		}
		return nil
	})
	if err != nil {
		fmt.Printf("Error during file processing: %v\n", err)
		return
	}

	fmt.Println("All files processed successfully.")
}
