package bitswap

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

const testSeed = 99

func TestInternetLatencyDelayNextWaitTimeDistribution(t *testing.T) {
	initialValue := 1000 * time.Millisecond
	deviation := 100 * time.Millisecond
	mediumDelay := 1000 * time.Millisecond
	largeDelay := 3000 * time.Millisecond
	percentMedium := 0.2
	percentLarge := 0.4
	buckets := make(map[string]int)
	internetLatencyDistributionDelay := InternetLatencyDelayGenerator(
		mediumDelay,
		largeDelay,
		percentMedium,
		percentLarge,
		deviation,
		rand.New(rand.NewSource(testSeed)))

	buckets["fast"] = 0
	buckets["medium"] = 0
	buckets["slow"] = 0
	buckets["outside_1_deviation"] = 0

	// strategy here is rather than mock randomness, just use enough samples to
	// get approximately the distribution you'd expect
	for i := 0; i < 10000; i++ {
		next := internetLatencyDistributionDelay.NextWaitTime(initialValue)
		if math.Abs((next - initialValue).Seconds()) <= deviation.Seconds() {
			buckets["fast"]++
		} else if math.Abs((next - initialValue - mediumDelay).Seconds()) <= deviation.Seconds() {
			buckets["medium"]++
		} else if math.Abs((next - initialValue - largeDelay).Seconds()) <= deviation.Seconds() {
			buckets["slow"]++
		} else {
			buckets["outside_1_deviation"]++
		}
	}
	totalInOneDeviation := float64(10000 - buckets["outside_1_deviation"])
	oneDeviationPercentage := totalInOneDeviation / 10000
	fastPercentageResult := float64(buckets["fast"]) / totalInOneDeviation
	mediumPercentageResult := float64(buckets["medium"]) / totalInOneDeviation
	slowPercentageResult := float64(buckets["slow"]) / totalInOneDeviation

	// see 68-95-99 rule for normal distributions
	if math.Abs(oneDeviationPercentage-0.6827) >= 0.1 {
		t.Fatal("Failed to distribute values normally based on standard deviation")
	}

	if math.Abs(fastPercentageResult+percentMedium+percentLarge-1) >= 0.1 {
		t.Fatal("Incorrect percentage of values distributed around fast delay time")
	}

	if math.Abs(mediumPercentageResult-percentMedium) >= 0.1 {
		t.Fatal("Incorrect percentage of values distributed around medium delay time")
	}

	if math.Abs(slowPercentageResult-percentLarge) >= 0.1 {
		t.Fatal("Incorrect percentage of values distributed around slow delay time")
	}
}
