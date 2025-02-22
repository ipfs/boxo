package builder

import "fmt"

// Progress represents the current progress of a file/directory add operation
type Progress struct {
	Path           string
	BytesProcessed int64
	TotalBytes     int64
	Operation      string // "file", "directory", "chunk"
}

// ProgressFunc is a callback function for progress updates
type ProgressFunc func(Progress)

// humanReadableSize converts bytes to human readable format
func HumanReadableSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
