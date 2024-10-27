package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/term"
)

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

var (
	userAgents = []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
	}
	errorLogMutex sync.Mutex
)

type ConsoleDisplay struct {
	bar          *pb.ProgressBar
	tagStats     map[string]int64
	statusStats  map[int]int64
	mu           sync.RWMutex
	termHeight   int
	termWidth    int
	lastUpdate   time.Time
	startTime    time.Time
	updateRate   time.Duration
	statLines    int
	started      bool
	speed        float64
	processed    int64
	total        int64
	currentBatch int
	totalBatches int
}

func (cd *ConsoleDisplay) SetBatch(current int) {
	cd.mu.Lock()
	cd.currentBatch = current
	cd.mu.Unlock()
}
func NewConsoleDisplay(total int, totalBatches int) *ConsoleDisplay {
	bar := pb.New(total)
	bar.SetTemplate(``)
	bar.Set("prefix", "")
	bar.SetRefreshRate(time.Second)

	return &ConsoleDisplay{
		bar:          bar,
		tagStats:     make(map[string]int64),
		statusStats:  make(map[int]int64),
		updateRate:   time.Second,
		termWidth:    100,
		total:        int64(total),
		startTime:    time.Now(),
		totalBatches: totalBatches,
		currentBatch: 1,
	}
}

func (cd *ConsoleDisplay) UpdateStatus(statusCode int) {
	cd.mu.Lock()
	cd.statusStats[statusCode]++
	cd.mu.Unlock()
}

// Add a helper method to format numbers with commas
func formatNumber(n int64) string {
	str := fmt.Sprintf("%d", n)
	if len(str) < 4 {
		return str
	}

	var result []byte
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

func (cd *ConsoleDisplay) Start() {
	fmt.Print("\033[2J\033[H")
	cd.started = true
	cd.bar.Start()
	go cd.monitorTerminalSize()
}
func (cd *ConsoleDisplay) monitorTerminalSize() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		if width, height, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
			cd.mu.Lock()
			cd.termWidth = width
			cd.termHeight = height
			cd.mu.Unlock()
		}
	}
}

const (
	topLeft     = "+"
	topRight    = "+"
	bottomLeft  = "+"
	bottomRight = "+"
	horizontal  = "-"
	vertical    = "|"
)

func (cd *ConsoleDisplay) UpdateTagStats(tags map[string]bool) {
	cd.mu.Lock()
	defer cd.mu.Unlock()

	for tag := range tags {
		cd.tagStats[tag]++
	}

	if time.Since(cd.lastUpdate) < cd.updateRate {
		return
	}
	cd.lastUpdate = time.Now()

	if cd.statLines > 0 {
		fmt.Printf("\033[%dA\033[J", cd.statLines)
	}

	type tagCount struct {
		tag   string
		count int64
	}
	var sortedTags []tagCount
	for tag, cnt := range cd.tagStats {
		sortedTags = append(sortedTags, tagCount{tag, cnt})
	}
	sort.Slice(sortedTags, func(i, j int) bool {
		return sortedTags[i].count > sortedTags[j].count
	})

	const (
		minWidth = 80
		tagWidth = 20
	)

	displayWidth := minWidth
	if cd.termWidth > minWidth {
		displayWidth = cd.termWidth
	}

	var eta string
	if cd.speed > 0 {
		remaining := cd.total - cd.processed
		remainingSeconds := (remaining * 60) / int64(cd.speed)
		if remainingSeconds < 60 {
			eta = fmt.Sprintf("%ds", remainingSeconds)
		} else {
			minutes := remainingSeconds / 60
			seconds := remainingSeconds % 60
			eta = fmt.Sprintf("%dm%ds", minutes, seconds)
		}
	} else {
		eta = "calculating..."
	}

	elapsed := time.Since(cd.startTime)
	elapsedStr := fmt.Sprintf("%dm%ds", int(elapsed.Minutes()), int(elapsed.Seconds())%60)

	total200and503andUnreachable := cd.statusStats[200] + cd.statusStats[503] + cd.statusStats[-3]
	var successRate float64
	if total200and503andUnreachable > 0 {
		successRate = float64(cd.statusStats[200]) / float64(total200and503andUnreachable) * 100
	}

	fmt.Printf("\n+%s+\n", strings.Repeat("-", displayWidth-2))

	progressLine := fmt.Sprintf("Batch: %d/%d | Time: %s | Progress: %d/%d (%.2f%%) Speed: %.0f URLs/m ETA: %s",
		cd.currentBatch, cd.totalBatches, elapsedStr,
		cd.processed, cd.total, float64(cd.processed)/float64(cd.total)*100, cd.speed, eta)
	fmt.Printf("| %-*s |\n", displayWidth-4, progressLine)

	statusLine := fmt.Sprintf("Status: 200: %d | -3: %d | 503: %d | Success Rate: %.2f%%",
		cd.statusStats[200], cd.statusStats[-3], cd.statusStats[503], successRate)
	fmt.Printf("| %-*s |\n", displayWidth-4, statusLine)

	fmt.Printf("+%s+\n", strings.Repeat("-", displayWidth-2))

	var currentLine strings.Builder
	count := 0
	for _, tc := range sortedTags {
		if count > 0 && count%4 == 0 {
			padding := displayWidth - 4 - currentLine.Len()
			if padding > 0 {
				currentLine.WriteString(strings.Repeat(" ", padding))
			}
			fmt.Printf("| %s |\n", currentLine.String())
			currentLine.Reset()
		}

		tagText := fmt.Sprintf("%s (%d)", tc.tag, tc.count)
		if len(tagText) > tagWidth-2 {
			tagText = tagText[:tagWidth-5] + "..."
		}
		currentLine.WriteString(fmt.Sprintf("%-*s", tagWidth, tagText))
		count++
	}

	if currentLine.Len() > 0 {
		padding := displayWidth - 4 - currentLine.Len()
		if padding > 0 {
			currentLine.WriteString(strings.Repeat(" ", padding))
		}
		fmt.Printf("| %s |\n", currentLine.String())
	}

	fmt.Printf("+%s+\n", strings.Repeat("-", displayWidth-2))

	cd.statLines = (count+3)/4 + 7
}

func (cd *ConsoleDisplay) Clear() {
	cd.mu.Lock()
	defer cd.mu.Unlock()

	if cd.statLines > 0 {
		fmt.Printf("\033[%dA\033[J", cd.statLines)
		cd.statLines = 0
	}
}

func (cd *ConsoleDisplay) Increment() {
	cd.bar.Increment()
}

func (cd *ConsoleDisplay) SetCurrent(current int64) {
	cd.mu.Lock()
	cd.processed = current
	cd.mu.Unlock()
	cd.bar.SetCurrent(current)
}
func (cd *ConsoleDisplay) SetSpeed(speed float64) {
	cd.mu.Lock()
	cd.speed = speed
	cd.mu.Unlock()
}

func (cd *ConsoleDisplay) Finish() {
	cd.bar.Finish()
}

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
	cacheDir          = "cache"
	maxCacheSize      = 10000
	cacheCleanupRatio = 0.5
)

type CacheManager struct {
	baseDir     string
	maxSize     int
	cleanupLock sync.Mutex
	processMu   sync.Mutex
}

func NewCacheManager() *CacheManager {
	return &CacheManager{
		baseDir: cacheDir,
		maxSize: maxCacheSize,
	}
}

func (cm *CacheManager) CleanAll() error {
	cm.processMu.Lock()
	defer cm.processMu.Unlock()

	if err := os.RemoveAll(filepath.Join(cm.baseDir, "cache")); err != nil {
		return err
	}

	return os.MkdirAll(filepath.Join(cm.baseDir, "cache"), 0755)
}
func (cm *CacheManager) EnsureAllProcessed() error {
	cm.processMu.Lock()
	defer cm.processMu.Unlock()

	cacheDir := filepath.Join(cm.baseDir, "cache")
	files, err := os.ReadDir(cacheDir)
	if err != nil {
		return fmt.Errorf("failed to read cache directory: %v", err)
	}

	if len(files) > 0 {
		for _, file := range files {
			if !file.IsDir() {
				cachePath := filepath.Join(cacheDir, file.Name())
				if err := os.Remove(cachePath); err != nil {
					log.Warn().Err(err).Str("file", file.Name()).Msg("Failed to remove remaining cache file")
				}
			}
		}

		return fmt.Errorf("found and removed %d unprocessed files in cache", len(files))
	}

	return nil
}

func (cm *CacheManager) Initialize() error {
	if err := os.MkdirAll(filepath.Join(cm.baseDir, "cache"), 0755); err != nil {
		return fmt.Errorf("failed to create cache directory: %v", err)
	}

	return cm.CleanAll()
}

func (cm *CacheManager) GetCacheFilePath(url string, _ string) string {
	safeURL := strings.Map(func(r rune) rune {
		if strings.ContainsRune(`<>:"/\|?*`, r) {
			return '_'
		}
		return r
	}, url)

	return filepath.Join(cm.baseDir, "cache", safeURL+".html")
}
func (cm *CacheManager) MarkAsProcessed(url string) error {
	cm.processMu.Lock()
	defer cm.processMu.Unlock()

	cachePath := cm.GetCacheFilePath(url, "cache")

	if err := os.Remove(cachePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove processed file: %v", err)
	}

	return nil
}

func (cm *CacheManager) CleanProcessedFiles() error {
	processedDir := filepath.Join(cm.baseDir, "processed")
	return os.RemoveAll(processedDir)
}

func (cm *CacheManager) SaveContent(url string, content []byte, statusCode int) error {
	cm.processMu.Lock()
	defer cm.processMu.Unlock()

	cachePath := cm.GetCacheFilePath(url, "cache")

	fullContent := fmt.Sprintf("%d\n%s", statusCode, content)
	if err := os.WriteFile(cachePath, []byte(fullContent), 0644); err != nil {
		return err
	}

	go cm.checkAndCleanup()
	return nil
}

func (cm *CacheManager) checkAndCleanup() {
	cm.cleanupLock.Lock()
	defer cm.cleanupLock.Unlock()

	cacheDir := filepath.Join(cm.baseDir, "cache")
	files, err := os.ReadDir(cacheDir)
	if err != nil {
		log.Error().Err(err).Msg("Failed to read cache directory")
		return
	}

	if len(files) > cm.maxSize {
		type fileInfo struct {
			path    string
			modTime time.Time
		}
		var filesInfo []fileInfo

		for _, f := range files {
			info, err := f.Info()
			if err != nil {
				continue
			}
			filesInfo = append(filesInfo, fileInfo{
				path:    filepath.Join(cacheDir, f.Name()),
				modTime: info.ModTime(),
			})
		}

		sort.Slice(filesInfo, func(i, j int) bool {
			return filesInfo[i].modTime.Before(filesInfo[j].modTime)
		})

		numToDelete := int(float64(len(filesInfo)) * cacheCleanupRatio)
		deleted := 0

		for i := 0; i < numToDelete && i < len(filesInfo); i++ {
			if err := os.Remove(filesInfo[i].path); err != nil {
				log.Warn().
					Err(err).
					Str("file", filesInfo[i].path).
					Msg("Failed to remove file during cleanup")
				continue
			}
			deleted++
		}

		log.Info().
			Int("removed", deleted).
			Int("remaining", len(filesInfo)-deleted).
			Msg("Cache cleanup completed")
	}
}

type FileWriter struct {
	basePath string
	mu       sync.Mutex
}

func NewFileWriter(basePath string) *FileWriter {
	return &FileWriter{
		basePath: basePath,
	}
}

// WriteURLByStatus writes a URL to its corresponding status code file
func (fw *FileWriter) WriteURLByStatus(statusCode int, url string) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	statusDir := filepath.Join(fw.basePath, "Archive", time.Now().Format("2006-01-02"), "statuses")
	if err := os.MkdirAll(statusDir, 0755); err != nil {
		return fmt.Errorf("failed to create status directory: %v", err)
	}

	filename := filepath.Join(statusDir, fmt.Sprintf("%d.txt", statusCode))
	return appendToFile(filename, url)
}

// WriteURLByTag writes a URL to its corresponding tag file
func (fw *FileWriter) WriteURLByTag(tag string, url string) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	tagDir := filepath.Join(fw.basePath, "Archive", time.Now().Format("2006-01-02"))
	if err := os.MkdirAll(tagDir, 0755); err != nil {
		return fmt.Errorf("failed to create tag directory: %v", err)
	}

	filename := filepath.Join(tagDir, fmt.Sprintf("%s.txt", tag))
	return appendToFile(filename, url)
}

type ArchiveManager struct {
	basePath    string
	stats       *ArchiveStats
	matcher     *AhoCorasick
	tagPatterns map[string][]string
	fileWriter  *FileWriter
	mu          sync.RWMutex
}

// Modify DownloadResult to include cache information
type DownloadResult struct {
	URL          string
	StatusCode   int
	Size         int64
	ErrorMessage string
	RetryCount   int
	Timestamp    time.Time
	Tags         map[string]bool
	FromCache    bool
	Content      []byte
}

type ArchiveStats struct {
	TimeStarted         time.Time
	TimeEnded           *time.Time
	TotalProcessed      int64
	IterationStats      map[int]*IterationStat
	RetryStats          sync.Map
	StatusRetries       sync.Map
	CurrentIteration    int
	TotalIterations     int
	InitialURLCount     int
	ProcessingSpeeds    []float64
	RecentResults       []DownloadResult
	StatusCodes         sync.Map
	TagCounts           sync.Map
	URLsByStatus        map[int][]string
	URLsByTag           map[string][]string
	TotalSize           int64
	Errors              sync.Map
	mu                  sync.RWMutex
	CurrentBatchStarted time.Time
}
type IterationStat struct {
	StatusCodes  map[int]int64
	TagCounts    map[string]int64
	URLsByStatus map[int][]string
	URLsByTag    map[string][]string
	Processed    int64
	TotalSize    int64
}
type AhoCorasick struct {
}

func NewArchiveManager(outputDir string, totalIterations int) (*ArchiveManager, error) {
	if outputDir == "" {
		outputDir = "."
	}

	_, tagMap, matcher, err := loadTagPatterns()
	if err != nil {
		return nil, fmt.Errorf("failed to load tag patterns: %v", err)
	}

	return &ArchiveManager{
		basePath:    outputDir,
		stats:       NewArchiveStats(totalIterations, 0),
		matcher:     matcher,
		tagPatterns: tagMap,
		fileWriter:  NewFileWriter(outputDir),
	}, nil
}

func loadTagPatterns() ([]string, map[string][]string, *AhoCorasick, error) {
	execPath, err := os.Executable()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get executable path: %v", err)
	}

	tagsPath := filepath.Join(filepath.Dir(execPath), "tags.json")
	file, err := os.Open(tagsPath)
	if err != nil {
		file, err = os.Open("tags.json")
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to open tags.json: %v", err)
		}
	}
	defer file.Close()

	var tagMap map[string][]string
	if err := json.NewDecoder(file).Decode(&tagMap); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse tags.json: %v", err)
	}

	log.Info().Interface("tagPatterns", tagMap).Msg("Loaded tag patterns")

	var patterns []string
	for _, patternList := range tagMap {
		patterns = append(patterns, patternList...)
	}

	matcher := &AhoCorasick{}
	return patterns, tagMap, matcher, nil
}
func (am *ArchiveManager) ProcessContent(content []byte) map[string]bool {
	am.mu.RLock()
	defer am.mu.RUnlock()

	matches := make(map[string]bool)
	contentStr := strings.ToLower(string(content))

	if len(am.tagPatterns) == 0 {
		log.Warn().Msg("No tag patterns loaded!")
	}

	for tag, patterns := range am.tagPatterns {
		for _, pattern := range patterns {
			if strings.Contains(contentStr, strings.ToLower(pattern)) {
				matches[tag] = true
				log.Debug().Str("tag", tag).Str("pattern", pattern).Msg("Found tag match")
				break
			}
		}
	}

	return matches
}

func (am *ArchiveManager) PeriodicSave() error {
	am.stats.mu.Lock()
	defer am.stats.mu.Unlock()

	return am.stats.WriteArchiveReports(am.basePath)
}

func (am *ArchiveManager) Finalize() error {
	am.stats.mu.Lock()
	defer am.stats.mu.Unlock()

	now := time.Now()
	am.stats.TimeEnded = &now
	return am.stats.WriteArchiveReports(am.basePath)
}

func NewArchiveStats(totalIterations, initialURLCount int) *ArchiveStats {
	return &ArchiveStats{
		TimeStarted:         time.Now(),
		CurrentBatchStarted: time.Now(),
		IterationStats:      make(map[int]*IterationStat),
		TotalIterations:     totalIterations,
		InitialURLCount:     initialURLCount,
		ProcessingSpeeds:    make([]float64, 0),
		RecentResults:       make([]DownloadResult, 0),
		URLsByStatus:        make(map[int][]string),
		URLsByTag:           make(map[string][]string),
	}
}
func (s *ArchiveStats) Update(result DownloadResult, fileWriter *FileWriter) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.IterationStats[s.CurrentIteration] == nil {
		s.IterationStats[s.CurrentIteration] = &IterationStat{
			StatusCodes:  make(map[int]int64),
			TagCounts:    make(map[string]int64),
			URLsByStatus: make(map[int][]string),
			URLsByTag:    make(map[string][]string),
		}
	}

	iterStats := s.IterationStats[s.CurrentIteration]

	if result.ErrorMessage != "" {
		if val, ok := s.Errors.Load(result.ErrorMessage); ok {
			s.Errors.Store(result.ErrorMessage, val.(int64)+1)
		} else {
			s.Errors.Store(result.ErrorMessage, int64(1))
		}
	}

	iterStats.StatusCodes[result.StatusCode]++
	iterStats.Processed++
	iterStats.TotalSize += result.Size

	atomic.AddInt64(&s.TotalSize, result.Size)
	atomic.AddInt64(&s.TotalProcessed, 1)

	if val, ok := s.StatusCodes.Load(result.StatusCode); ok {
		s.StatusCodes.Store(result.StatusCode, val.(int64)+1)
	} else {
		s.StatusCodes.Store(result.StatusCode, int64(1))
	}

	iterStats.URLsByStatus[result.StatusCode] = append(
		iterStats.URLsByStatus[result.StatusCode],
		result.URL,
	)

	s.URLsByStatus[result.StatusCode] = append(
		s.URLsByStatus[result.StatusCode],
		result.URL,
	)

	if err := fileWriter.WriteURLByStatus(result.StatusCode, result.URL); err != nil {
		log.Error().Err(err).Msg("Failed to write URL to status file")
	}

	for tag := range result.Tags {
		iterStats.TagCounts[tag]++
		iterStats.URLsByTag[tag] = append(iterStats.URLsByTag[tag], result.URL)

		if val, ok := s.TagCounts.Load(tag); ok {
			s.TagCounts.Store(tag, val.(int64)+1)
		} else {
			s.TagCounts.Store(tag, int64(1))
		}

		s.URLsByTag[tag] = append(s.URLsByTag[tag], result.URL)

		if err := fileWriter.WriteURLByTag(tag, result.URL); err != nil {
			log.Error().Err(err).Msg("Failed to write URL to tag file")
		}
	}

	s.RecentResults = append(s.RecentResults, result)
	cutoff := time.Now().Add(-speedCalcWindow)
	for len(s.RecentResults) > 0 && s.RecentResults[0].Timestamp.Before(cutoff) {
		s.RecentResults = s.RecentResults[1:]
	}
	s.ProcessingSpeeds = append(s.ProcessingSpeeds, s.getCurrentSpeed())
}

func processChunk(ctx context.Context, urls []string, config Config, client *http.Client, am *ArchiveManager, cm *CacheManager) error {
	bar := createProgressBar(len(urls))
	defer bar.Finish()

	urlChan := make(chan string, config.NumThreads)
	results := make(chan DownloadResult, config.NumThreads)
	var wg sync.WaitGroup

	for i := 0; i < config.NumThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for url := range urlChan {
				select {
				case <-ctx.Done():
					return
				default:
					result := downloadURLWithRetry(client, url, config.MaxRetries, am, cm)
					results <- result
					bar.Increment()
				}
			}
		}()
	}

	go func() {
		for _, url := range urls {
			select {
			case <-ctx.Done():
				return
			case urlChan <- url:
			}
		}
		close(urlChan)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	processedURLs := make(map[string]bool)
	for result := range results {
		if !processedURLs[result.URL] {
			processedURLs[result.URL] = true
			am.stats.Update(result, am.fileWriter)
		}
	}

	return nil
}
func processIteration(ctx context.Context, iteration int, config Config, client *http.Client, am *ArchiveManager, cm *CacheManager) error {
	urls, err := loadURLs(config.InputFile, iteration)
	if err != nil {
		return fmt.Errorf("failed to load URLs: %v", err)
	}

	if len(urls) == 0 {
		return nil
	}

	am.stats.mu.Lock()
	am.stats.CurrentIteration = iteration
	am.stats.InitialURLCount = len(urls)
	am.stats.CurrentBatchStarted = time.Now()
	am.stats.mu.Unlock()

	batches := splitIntoBatches(urls, config.BatchSize)
	log.Info().Msgf("Split %d URLs into %d batches of size %d", len(urls), len(batches), config.BatchSize)

	display := NewConsoleDisplay(len(urls), len(batches))
	display.Start()
	defer display.Finish()

	for batchNum, batch := range batches {
		select {
		case <-ctx.Done():
			return nil
		default:
			log.Info().Msgf("Starting batch %d/%d (%d URLs)", batchNum+1, len(batches), len(batch))
			display.SetBatch(batchNum + 1)

			urlChan := make(chan string, len(batch))
			resultChan := make(chan DownloadResult, len(batch))
			var wg sync.WaitGroup

			// Start worker goroutines
			for i := 0; i < config.NumThreads; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for url := range urlChan {
						select {
						case <-ctx.Done():
							return
						default:
							result := downloadURLWithRetry(client, url, config.MaxRetries, am, cm)
							resultChan <- result
							// Display updates should be synchronized to avoid race conditions
							display.Increment()
							display.UpdateStatus(result.StatusCode)

							if len(result.Tags) > 0 {
								display.UpdateTagStats(result.Tags)
							}
							displayProgress(am.stats, display)
						}
					}
				}()
			}

			// Feed URLs into the channel
			go func() {
				for _, url := range batch {
					select {
					case urlChan <- url:
					case <-ctx.Done():
						close(urlChan) // Ensure channel is closed if context is done
						return
					}
				}
				close(urlChan) // Close urlChan after all URLs are sent
			}()

			// Process results and update stats
			processedURLs := make(map[string]bool)
			go func() {
				for result := range resultChan {
					if !processedURLs[result.URL] {
						processedURLs[result.URL] = true
						am.stats.Update(result, am.fileWriter)
					}
				}
			}()

			// Wait for all workers to finish
			wg.Wait()
			close(resultChan) // Close resultChan after workers are done

			if err := am.PeriodicSave(); err != nil {
				log.Warn().Err(err).Msgf("Failed to save progress after batch %d", batchNum+1)
			}

			log.Info().Msgf("Completed batch %d/%d", batchNum+1, len(batches))
		}
	}

	return nil
}

func (s *ArchiveStats) getCurrentSpeed() float64 {
	elapsed := time.Since(s.TimeStarted).Minutes()
	if elapsed > 0 {
		return float64(s.TotalProcessed) / elapsed
	}
	return 0
}

func (s *ArchiveStats) WriteArchiveReports(basePath string) error {
	date := time.Now().Format("2006-01-02")
	archivePath := filepath.Join(basePath, "Archive", date)

	if err := os.MkdirAll(archivePath, 0755); err != nil {
		return fmt.Errorf("failed to create archive directory: %v", err)
	}

	if err := s.writeTotalReport(archivePath); err != nil {
		return err
	}

	if err := s.writeTagFiles(archivePath); err != nil {
		return err
	}

	if err := s.writeStatusFiles(archivePath); err != nil {
		return err
	}

	if err := s.writePerformanceReport(archivePath); err != nil {
		return err
	}

	if err := s.writeIterationReports(archivePath); err != nil {
		return err
	}

	return nil
}
func (s *ArchiveStats) writeIterationReports(path string) error {
	iterationsDir := filepath.Join(path, "iterations")
	if err := os.MkdirAll(iterationsDir, 0755); err != nil {
		return err
	}

	file, err := os.Create(filepath.Join(iterationsDir, "iterations_summary.txt"))
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	defer w.Flush()

	fmt.Fprintf(w, "=== Iterations Summary Report ===\n\n")

	for i := 1; i <= s.CurrentIteration; i++ {
		iterStats := s.IterationStats[i]
		if iterStats == nil {
			continue
		}

		fmt.Fprintf(w, "=== Iteration %d ===\n", i)
		fmt.Fprintf(w, "Status Code Distribution:\n")

		var codes []int
		for code := range iterStats.StatusCodes {
			codes = append(codes, code)
		}
		sort.Ints(codes)

		for _, code := range codes {
			count := iterStats.StatusCodes[code]
			percentage := float64(count) / float64(iterStats.Processed) * 100
			fmt.Fprintf(w, "  HTTP %d: %d (%.2f%%)\n", code, count, percentage)
		}

		fmt.Fprintf(w, "\nTags Found:\n")
		for tag, count := range iterStats.TagCounts {
			successCount := 0
			if urls, ok := iterStats.URLsByTag[tag]; ok {
				for _, u := range urls {
					if containsURL(iterStats.URLsByStatus[200], u) {
						successCount++
					}
				}
			}
			successRate := float64(successCount) / float64(count) * 100
			fmt.Fprintf(w, "  %s: %d hits (%.1f%% success rate)\n", tag, count, successRate)
		}

		fmt.Fprintf(w, "\n")
	}

	return nil
}
func (s *ArchiveStats) writeStatusReport(path string) error {
	file, err := os.Create(filepath.Join(path, fmt.Sprintf("status_codes_%d.txt", s.CurrentIteration)))
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	defer w.Flush()

	fmt.Fprintf(w, "=== Status Code Report (Iteration %d) ===\n\n", s.CurrentIteration)

	statusCounts := make(map[int]int64)
	s.StatusCodes.Range(func(key, value interface{}) bool {
		statusCounts[key.(int)] = value.(int64)
		return true
	})

	var codes []int
	for code := range statusCounts {
		codes = append(codes, code)
	}
	sort.Ints(codes)

	for _, code := range codes {
		count := statusCounts[code]
		urls := s.URLsByStatus[code]

		fmt.Fprintf(w, "HTTP %d:\n", code)
		fmt.Fprintf(w, "  Count: %d\n", count)
		fmt.Fprintf(w, "  URLs:\n")
		for _, url := range urls {
			fmt.Fprintf(w, "    %s\n", url)
		}
		fmt.Fprintf(w, "\n")
	}

	return nil
}
func (s *ArchiveStats) writeTagReport(path string) error {
	file, err := os.Create(filepath.Join(path, fmt.Sprintf("tag_analysis_%d.txt", s.CurrentIteration)))
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	defer w.Flush()

	fmt.Fprintf(w, "=== Tag Analysis Report (Iteration %d) ===\n\n", s.CurrentIteration)

	tagCounts := make(map[string]int64)
	s.TagCounts.Range(func(key, value interface{}) bool {
		tagCounts[key.(string)] = value.(int64)
		return true
	})

	type tagCount struct {
		tag   string
		count int64
	}
	var sortedTags []tagCount
	for tag, count := range tagCounts {
		sortedTags = append(sortedTags, tagCount{tag, count})
	}
	sort.Slice(sortedTags, func(i, j int) bool {
		return sortedTags[i].count > sortedTags[j].count
	})

	for _, tc := range sortedTags {
		successCount := 0
		urls := s.URLsByTag[tc.tag]
		for _, url := range urls {
			if containsURL(s.URLsByStatus[200], url) {
				successCount++
			}
		}
		successRate := float64(successCount) / float64(tc.count) * 100

		fmt.Fprintf(w, "Tag: %s\n", tc.tag)
		fmt.Fprintf(w, "  Total Hits: %d\n", tc.count)
		fmt.Fprintf(w, "  Success Rate: %.1f%%\n", successRate)
		fmt.Fprintf(w, "  URLs:\n")
		for _, url := range urls {
			fmt.Fprintf(w, "    %s\n", url)
		}
		fmt.Fprintf(w, "\n")
	}

	return nil
}
func (s *ArchiveStats) writeIterationReport(path string) error {
	file, err := os.Create(filepath.Join(path, fmt.Sprintf("iteration_%d_report.txt", s.CurrentIteration)))
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	defer w.Flush()

	iterStats := s.IterationStats[s.CurrentIteration]
	if iterStats == nil {
		return fmt.Errorf("no stats found for iteration %d", s.CurrentIteration)
	}

	fmt.Fprintf(w, "=== Iteration %d Report ===\n\n", s.CurrentIteration)
	fmt.Fprintf(w, "Time: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(w, "URLs Processed: %d\n", iterStats.Processed)
	fmt.Fprintf(w, "Data Downloaded: %.2f MB\n\n", float64(iterStats.TotalSize)/1024/1024)

	fmt.Fprintf(w, "Status Code Distribution:\n")
	var codes []int
	for code := range iterStats.StatusCodes {
		codes = append(codes, code)
	}
	sort.Ints(codes)

	for _, code := range codes {
		count := iterStats.StatusCodes[code]
		percentage := float64(count) / float64(iterStats.Processed) * 100
		fmt.Fprintf(w, "  HTTP %d: %d (%.2f%%)\n", code, count, percentage)
	}

	return nil
}
func (s *ArchiveStats) writeTotalReport(path string) error {
	file, err := os.Create(filepath.Join(path, "total_report.txt"))
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	defer w.Flush()

	fmt.Fprintf(w, "=== Web Analyzer Total Report ===\n\n")
	fmt.Fprintf(w, "Time Started: %s\n", s.TimeStarted.Format("2006-01-02 15:04:05"))
	if s.TimeEnded != nil {
		fmt.Fprintf(w, "Time Ended: %s\n", s.TimeEnded.Format("2006-01-02 15:04:05"))
		fmt.Fprintf(w, "Total Duration: %s\n", s.TimeEnded.Sub(s.TimeStarted))
	}

	fmt.Fprintf(w, "\nTotal Iterations: %d\n", s.TotalIterations)
	fmt.Fprintf(w, "Total URLs Processed: %d\n", atomic.LoadInt64(&s.TotalProcessed))
	fmt.Fprintf(w, "Total Data Downloaded: %.2f MB\n", float64(atomic.LoadInt64(&s.TotalSize))/1024/1024)
	fmt.Fprintf(w, "Average Processing Speed: %.2f URLs/minute\n", s.getCurrentSpeed())

	fmt.Fprintf(w, "\n=== Status Code Summary (All Iterations) ===\n")
	var codes []int
	s.StatusCodes.Range(func(key, value interface{}) bool {
		codes = append(codes, key.(int))
		return true
	})
	sort.Ints(codes)

	total := float64(atomic.LoadInt64(&s.TotalProcessed))
	for _, code := range codes {
		count, _ := s.StatusCodes.Load(code)
		percentage := float64(count.(int64)) / total * 100

		var desc string
		switch {
		case code >= 200 && code < 300:
			desc = "Success"
		case code >= 300 && code < 400:
			desc = "Redirect"
		case code >= 400 && code < 500:
			desc = "Client Error"
		case code >= 500 && code < 600:
			desc = "Server Error"
		default:
			desc = "Unknown"
		}

		fmt.Fprintf(w, "HTTP %d (%s): %d (%.2f%%)\n", code, desc, count.(int64), percentage)
	}

	fmt.Fprintf(w, "\n=== Tag Analysis Summary ===\n")
	s.TagCounts.Range(func(key, value interface{}) bool {
		tag := key.(string)
		count := value.(int64)

		successCount := 0
		if urls, ok := s.URLsByTag[tag]; ok {
			for _, u := range urls {
				if containsURL(s.URLsByStatus[200], u) {
					successCount++
				}
			}
		}
		successRate := float64(successCount) / float64(count) * 100
		fmt.Fprintf(w, "%s: %d hits (%.1f%% success rate)\n", tag, count, successRate)
		return true
	})

	return nil
}
func (s *ArchiveStats) writeTagFiles(path string) error {
	for tag, urls := range s.URLsByTag {
		tagPath := filepath.Join(path, fmt.Sprintf("%s.txt", tag))
		if err := writeLinesToFile(tagPath, urls); err != nil {
			return fmt.Errorf("failed to write tag file %s: %v", tag, err)
		}
	}
	return nil
}

func (s *ArchiveStats) writeStatusFiles(path string) error {
	statusDir := filepath.Join(path, "statuses")
	if err := os.MkdirAll(statusDir, 0755); err != nil {
		return err
	}

	for code, urls := range s.URLsByStatus {
		filename := filepath.Join(statusDir, fmt.Sprintf("%d.txt", code))
		if err := writeLinesToFile(filename, urls); err != nil {
			return fmt.Errorf("failed to write status file %d: %v", code, err)
		}
	}
	return nil
}

func (s *ArchiveStats) writePerformanceReport(path string) error {
	file, err := os.Create(filepath.Join(path, "performance.txt"))
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	defer w.Flush()

	fmt.Fprintf(w, "=== Performance Report ===\n\n")
	fmt.Fprintf(w, "Average Speed: %.2f URLs/minute\n", s.getCurrentSpeed())
	if len(s.ProcessingSpeeds) > 0 {
		fmt.Fprintf(w, "Peak Speed: %.2f URLs/minute\n", s.ProcessingSpeeds[len(s.ProcessingSpeeds)-1])
	}

	return nil
}

func (s *ArchiveStats) writeErrorReport(path string) error {
	file, err := os.Create(filepath.Join(path, "errors.txt"))
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	defer w.Flush()

	fmt.Fprintf(w, "=== Error Report ===\n\n")

	errorCounts := make(map[string]int64)
	s.Errors.Range(func(key, value interface{}) bool {
		errorCounts[key.(string)] = value.(int64)
		return true
	})

	var errorMessages []string
	for msg := range errorCounts {
		errorMessages = append(errorMessages, msg)
	}
	sort.Strings(errorMessages)

	for _, msg := range errorMessages {
		fmt.Fprintf(w, "%s: %d occurrences\n", msg, errorCounts[msg])
	}

	return nil
}

func createClient(timeout time.Duration, proxyURL string) (*http.Client, error) {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		MaxIdleConns:    100,
		IdleConnTimeout: 90 * time.Second,
	}

	if proxyURL != "" {
		proxyURLParsed, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy URL: %v", err)
		}
		transport.Proxy = http.ProxyURL(proxyURLParsed)
	}

	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}, nil
}
func downloadURLWithRetry(client *http.Client, urlStr string, retries int, am *ArchiveManager, cm *CacheManager) DownloadResult {
	cachePath := cm.GetCacheFilePath(urlStr, "cache")
	requestTimeout := 10 * time.Second

	// Set up a new HTTP client with the specified timeout
	clientWithTimeout := &http.Client{
		Timeout: requestTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // Adjust as necessary
		},
	}

	// Check if URL content is cached
	if content, err := os.ReadFile(cachePath); err == nil {
		parts := bytes.SplitN(content, []byte("\n"), 2)
		if len(parts) == 2 {
			statusCode := 0
			fmt.Sscanf(string(parts[0]), "%d", &statusCode)

			tags := am.ProcessContent(parts[1])

			if statusCode == 200 {
				if err := cm.MarkAsProcessed(urlStr); err != nil {
					log.Warn().Err(err).Str("url", urlStr).Msg("Failed to mark URL as processed from cache")
				}
			}

			return DownloadResult{
				URL:        urlStr,
				StatusCode: statusCode,
				Size:       int64(len(parts[1])),
				Content:    parts[1],
				Tags:       tags,
				FromCache:  true,
				Timestamp:  time.Now(),
			}
		}
	}

	result := DownloadResult{
		URL:       urlStr,
		Timestamp: time.Now(),
		Tags:      make(map[string]bool),
	}

	// Retry loop with timeout context
	for attempt := 0; attempt < retries; attempt++ {
		if attempt > 0 {
			time.Sleep(retryDelay)
			result.RetryCount++
		}

		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()

		downloadResult := downloadURLWithTimeout(ctx, clientWithTimeout, urlStr, am, cm)
		result = downloadResult

		if result.StatusCode == 200 && len(result.Content) > 0 {
			if err := cm.SaveContent(urlStr, result.Content, result.StatusCode); err != nil {
				log.Warn().Err(err).Str("url", urlStr).Msg("Failed to save content to cache")
			} else {
				if err := cm.MarkAsProcessed(urlStr); err != nil {
					log.Warn().Err(err).Str("url", urlStr).Msg("Failed to mark URL as processed after download")
				}
			}
			return result
		}

		// Handle rate limits with backoff
		if result.StatusCode == 429 || result.StatusCode == 530 {
			if attempt < maxRetriesFor429 {
				backoffTime := time.Duration(attempt*2) * time.Second
				time.Sleep(backoffTime)
				continue
			}
		}

		if result.ErrorMessage == "" || (!isNetworkError(result.ErrorMessage) && result.StatusCode != 429 && result.StatusCode != 520 && result.StatusCode != 530) {
			break
		}
	}

	// Log network errors if the download ultimately fails
	if isNetworkError(result.ErrorMessage) || result.StatusCode == 429 || result.StatusCode == 520 || result.StatusCode == 530 || result.StatusCode == 503 {
		appendToFile(networkErrorFile, urlStr)
	}

	return result
}

// downloadURLWithTimeout initiates a request with the provided context timeout.
func downloadURLWithTimeout(ctx context.Context, client *http.Client, urlStr string, am *ArchiveManager, cm *CacheManager) DownloadResult {
	req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
	if err != nil {
		return DownloadResult{URL: urlStr, ErrorMessage: err.Error()}
	}

	resp, err := client.Do(req)
	if err != nil {
		return DownloadResult{URL: urlStr, ErrorMessage: err.Error()}
	}
	defer resp.Body.Close()

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return DownloadResult{URL: urlStr, StatusCode: resp.StatusCode, ErrorMessage: err.Error()}
	}

	tags := am.ProcessContent(content)

	return DownloadResult{
		URL:        urlStr,
		StatusCode: resp.StatusCode,
		Size:       int64(len(content)),
		Content:    content,
		Tags:       tags,
		Timestamp:  time.Now(),
	}
}

func downloadURL(client *http.Client, urlStr string, am *ArchiveManager, cm *CacheManager) DownloadResult {

	normalizedURL := normalizeURL(urlStr)
	parsedURL, err := url.Parse(normalizedURL)
	if err != nil {
		return DownloadResult{
			URL:          urlStr,
			StatusCode:   -1,
			ErrorMessage: fmt.Sprintf("Invalid URL format: %v", err),
			Timestamp:    time.Now(),
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", normalizedURL, nil)
	if err != nil {
		return DownloadResult{
			URL:          urlStr,
			StatusCode:   -2,
			ErrorMessage: fmt.Sprintf("Request creation failed: %v", err),
			Timestamp:    time.Now(),
		}
	}

	setHeaders(req)
	req.Host = parsedURL.Host

	resp, err := client.Do(req)
	if err != nil {
		var errorMsg string
		switch {
		case os.IsTimeout(err):
			errorMsg = "Connection timed out"
		case strings.Contains(err.Error(), "no such host"):
			errorMsg = "Host not found"
		case strings.Contains(err.Error(), "connection refused"):
			errorMsg = "Connection refused"
		case strings.Contains(err.Error(), "no route to host"):
			errorMsg = "No route to host"
		case strings.Contains(err.Error(), "certificate"):
			errorMsg = fmt.Sprintf("SSL/TLS error: %v", err)
		default:
			if netErr, ok := err.(net.Error); ok {
				if netErr.Timeout() {
					errorMsg = "Request timed out"
				} else {
					errorMsg = fmt.Sprintf("Network error: %v", err)
				}
			} else {
				errorMsg = fmt.Sprintf("Request failed: %v", err)
			}
		}
		return DownloadResult{
			URL:          urlStr,
			StatusCode:   -3,
			ErrorMessage: errorMsg,
			Timestamp:    time.Now(),
		}
	}
	defer resp.Body.Close()

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return DownloadResult{
			URL:          urlStr,
			StatusCode:   resp.StatusCode,
			ErrorMessage: fmt.Sprintf("Failed to read response: %v", err),
			Timestamp:    time.Now(),
		}
	}

	tags := am.ProcessContent(content)

	return DownloadResult{
		URL:        urlStr,
		StatusCode: resp.StatusCode,
		Size:       int64(len(content)),
		Content:    content,
		Tags:       tags,
		Timestamp:  time.Now(),
	}
}

func setHeaders(req *http.Request) {
	req.Header.Set("User-Agent", getRandomUserAgent())
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Cache-Control", "max-age=0")
	req.Header.Set("Sec-Ch-Ua", "\"Google Chrome\";v=\"129\", \"Not_A Brand\";v=\"8\", \"Chromium\";v=\"129\"")
	req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
	req.Header.Set("Sec-Ch-Ua-Platform", "\"Windows\"")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-User", "?1")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
}
func displayProgress(stats *ArchiveStats, display *ConsoleDisplay) {
	stats.mu.RLock()
	defer stats.mu.RUnlock()

	elapsed := time.Since(stats.CurrentBatchStarted).Minutes()
	if elapsed > 0 {
		iterStats := stats.IterationStats[stats.CurrentIteration]
		if iterStats != nil {
			currentSpeed := float64(iterStats.Processed) / elapsed
			display.SetCurrent(int64(iterStats.Processed))
			display.SetSpeed(currentSpeed)
		}
	}
}
func createProgressBar(total int) *pb.ProgressBar {
	bar := pb.New(total)
	bar.SetTemplate(`{{ string . "prefix" }} {{ counters . }} {{ bar . }} {{ percent . }} {{ string . "speed" }} URLs/m {{ rtime . "ETA %s"}}`)
	bar.Set("prefix", "Processing")
	bar.SetMaxWidth(100)
	bar.Set(pb.Terminal, true)
	bar.SetRefreshRate(time.Second)
	bar.Start()
	return bar
}

// Split URLs into batches

// Split URLs into batches
func splitIntoBatches(urls []string, batchSize int) [][]string {
	var batches [][]string
	for i := 0; i < len(urls); i += batchSize {
		end := i + batchSize
		if end > len(urls) {
			end = len(urls)
		}
		batches = append(batches, urls[i:end])
	}
	return batches
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
	flag.IntVar(&config.BatchSize, "batch-size", 1000, "Size of URL batches to process")

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

func loadURLs(filename string, iteration int) ([]string, error) {
	if iteration > 1 {
		return read503URLs()
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var urls []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if url := strings.TrimSpace(scanner.Text()); url != "" {
			urls = append(urls, url)
		}
	}

	return urls, scanner.Err()
}
func read503URLs() ([]string, error) {
	filename := filepath.Join("statuses", "503.txt")
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, nil
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var urls []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if url := strings.TrimSpace(scanner.Text()); url != "" {
			urls = append(urls, url)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if err := os.WriteFile(filename, []byte{}, 0644); err != nil {
		log.Warn().Err(err).Msgf("Failed to clear %s", filename)
	}

	log.Info().Msgf("Loaded %d URLs from 503.txt for retry", len(urls))
	return urls, nil
}
func normalizeURL(urlStr string) string {
	urlStr = strings.TrimSpace(urlStr)
	if !strings.HasPrefix(urlStr, "http://") && !strings.HasPrefix(urlStr, "https://") {
		urlStr = "http://" + urlStr
	}
	return urlStr
}

func getRandomUserAgent() string {
	return userAgents[rand.Intn(len(userAgents))]
}

func isNetworkError(errMsg string) bool {
	return strings.Contains(errMsg, "Network error") ||
		strings.Contains(errMsg, "Request timed out") ||
		strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "no such host")
}

func logError(url, errorMessage string) {
	errorLogMutex.Lock()
	defer errorLogMutex.Unlock()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	errorEntry := fmt.Sprintf("[%s] URL: %s | Error: %s", timestamp, url, errorMessage)

	if err := appendToFile(errorLogFile, errorEntry); err != nil {
		log.Warn().Err(err).Msg("Failed to log error")
	}
}

func appendToFile(filename, content string) error {
	// Create directory only when we're actually writing to a file
	dir := filepath.Dir(filename)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %v", err)
		}
	}

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	if _, err := file.WriteString(content + "\n"); err != nil {
		return fmt.Errorf("failed to write to file: %v", err)
	}
	return nil
}

func writeLinesToFile(filename string, lines []string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, line := range lines {
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func containsURL(urls []string, target string) bool {
	for _, url := range urls {
		if url == target {
			return true
		}
	}
	return false
}

func setupLogging() {
	logLevel := zerolog.InfoLevel
	zerolog.SetGlobalLevel(logLevel)

	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339,
	})
}

func main() {
	setupLogging()
	config := parseFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm := NewCacheManager()
	if err := cm.Initialize(); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize cache")
	}

	if err := run(ctx, config, cm); err != nil {
		log.Error().Err(err).Msg("Error")
		os.Exit(1)
	}
}

func downloadURLDirect(client *http.Client, urlStr string, am *ArchiveManager, cm *CacheManager) DownloadResult {
	normalizedURL := normalizeURL(urlStr)
	parsedURL, err := url.Parse(normalizedURL)
	if err != nil {
		return DownloadResult{
			URL:          urlStr,
			StatusCode:   -1,
			ErrorMessage: fmt.Sprintf("Invalid URL format: %v", err),
			Timestamp:    time.Now(),
		}
	}

	req, err := http.NewRequest("GET", normalizedURL, nil)
	if err != nil {
		return DownloadResult{
			URL:          urlStr,
			StatusCode:   -2,
			ErrorMessage: fmt.Sprintf("Request creation failed: %v", err),
			Timestamp:    time.Now(),
		}
	}

	setHeaders(req)
	req.Host = parsedURL.Host

	resp, err := client.Do(req)
	if err != nil {
		var errorMsg string
		switch {
		case os.IsTimeout(err):
			errorMsg = "Connection timed out"
		case strings.Contains(err.Error(), "no such host"):
			errorMsg = "Host not found"
		case strings.Contains(err.Error(), "connection refused"):
			errorMsg = "Connection refused"
		case strings.Contains(err.Error(), "no route to host"):
			errorMsg = "No route to host"
		case strings.Contains(err.Error(), "certificate"):
			errorMsg = fmt.Sprintf("SSL/TLS error: %v", err)
		default:
			if netErr, ok := err.(net.Error); ok {
				if netErr.Timeout() {
					errorMsg = "Request timed out"
				} else {
					errorMsg = fmt.Sprintf("Network error: %v", err)
				}
			} else {
				errorMsg = fmt.Sprintf("Request failed: %v", err)
			}
		}
		return DownloadResult{
			URL:          urlStr,
			StatusCode:   -3,
			ErrorMessage: errorMsg,
			Timestamp:    time.Now(),
		}
	}
	defer resp.Body.Close()

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return DownloadResult{
			URL:          urlStr,
			StatusCode:   resp.StatusCode,
			ErrorMessage: fmt.Sprintf("Failed to read response: %v", err),
			Timestamp:    time.Now(),
		}
	}

	return DownloadResult{
		URL:        urlStr,
		StatusCode: resp.StatusCode,
		Size:       int64(len(content)),
		Content:    content,
		Tags:       am.ProcessContent(content),
		Timestamp:  time.Now(),
	}
}
func run(ctx context.Context, config Config, cm *CacheManager) error {
	if config.OutputDir != "downloaded_files" {
		if err := os.MkdirAll(config.OutputDir, 0755); err != nil {
			return fmt.Errorf("failed to create output directory: %v", err)
		}
	}

	archiveManager, err := NewArchiveManager(config.OutputDir, config.Iterations)
	if err != nil {
		return fmt.Errorf("failed to create archive manager: %v", err)
	}

	client, err := createClient(config.Timeout, config.ProxyURL)
	if err != nil {
		return fmt.Errorf("failed to create HTTP client: %v", err)
	}

	for iteration := 1; iteration <= config.Iterations; iteration++ {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := processIteration(ctx, iteration, config, client, archiveManager, cm); err != nil {
				return fmt.Errorf("iteration %d failed: %v", iteration, err)
			}

			if err := cm.EnsureAllProcessed(); err != nil {
				log.Warn().Err(err).Int("iteration", iteration).Msg("Found unprocessed files after iteration")
			}
		}
	}

	if err := cm.EnsureAllProcessed(); err != nil {
		log.Warn().Err(err).Msg("Found unprocessed files at end of run")
	}

	if err := archiveManager.Finalize(); err != nil {
		return fmt.Errorf("failed to finalize archive: %v", err)
	}

	if err := cm.CleanAll(); err != nil {
		log.Warn().Err(err).Msg("Failed to clean cache directories at end of run")
	}

	return nil
}
