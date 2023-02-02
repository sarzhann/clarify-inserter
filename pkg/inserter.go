package pkg

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/clarify/clarify-go"
	"github.com/clarify/clarify-go/fields"
	"github.com/clarify/clarify-go/views"
)

const (
	DefaultValue        = -999.9
	defaultSinFreq      = 60
	defaultSleepSeconds = 5
	defaultScale        = 100
	attempts            = 5
	seed                = 42
)

type Inserter struct {
	Client    *clarify.Client
	Frequency time.Duration
	Diff      time.Duration
	Timestamp fields.Timestamp
	Value     float64
	Method    string
	SignalID  string
	Items     int
	Size      int
	Sin       bool
	SinFreq   float64

	// internal variables
	randRatio float64
}

func (inserter *Inserter) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	inserter.init()
	var err error
	switch {
	case inserter.Method == "bucket":
		err = inserter.Bucket(ctx)
	case inserter.Method == "single":
		err = inserter.Single(ctx, inserter.Timestamp, inserter.Value)
	case inserter.Method == "stream":
		err = inserter.Stream(ctx)
	default:
		log.Fatal("method not in [single, stream, bucket]")
	}
	if err != nil {
		log.Fatal(err)
	}
}

func (inserter *Inserter) init() {
	inserter.Timestamp = fields.AsTimestamp(time.Now().Truncate(24 * time.Hour))
	if inserter.Sin {
		if inserter.SinFreq == 0 {
			switch {
			case inserter.Method == "bucket":
				inserter.SinFreq = float64(inserter.Size) / math.Sqrt(float64(inserter.Size))
			case inserter.Method == "stream":
				inserter.SinFreq = defaultSinFreq
			}
		}
		inserter.randRatio = 1 / math.Sqrt(inserter.SinFreq)
	}

}

func (inserter *Inserter) Stream(ctx context.Context) (err error) {
	ticker := time.NewTicker(inserter.Frequency)
	var counter int
	for {
		select {
		case <-ctx.Done():
			log.Println("context was closed")
		case <-ticker.C:
			var val float64
			switch {
			case inserter.Sin:
				x := math.Remainder(float64(counter), inserter.SinFreq) / float64(inserter.SinFreq)
				val = defaultScale * (math.Sin(2*math.Pi*x) + inserter.randRatio*rand.NormFloat64())
			case inserter.Value == DefaultValue:
				val = defaultScale * rand.NormFloat64()
			default:
				val = inserter.Value
			}

			ts := inserter.Timestamp.Add(time.Duration(counter) * inserter.Diff)
			if err = inserter.Single(ctx, ts, val); err != nil {
				return
			}
			counter++
		}
	}
}

func (inserter *Inserter) Single(ctx context.Context, ts fields.Timestamp, val float64) (err error) {
	df := views.DataFrame{inserter.SignalID: {ts: val}}

	if err = inserter.insert(ctx, df); err == nil {
		log.Printf("Inserted %.2f at %s\n", val, ts.Time().String())
	}
	return
}

func (inserter *Inserter) Bucket(ctx context.Context) (err error) {
	times := inserter.generateTimes()
	series := inserter.generateSeries()

	dfRaw := views.RawDataFrame{
		Times:  times,
		Series: series,
	}
	df := dfRaw.DataFrame()

	if err = inserter.insert(ctx, df); err == nil {
		log.Printf("Inserted %d items with %d data points", len(dfRaw.Series), len(dfRaw.Times))
	}
	return
}

func (inserter *Inserter) generateTimes() []fields.Timestamp {
	times := make([]fields.Timestamp, inserter.Size)
	for i := 0; i < inserter.Size; i++ {
		ts := inserter.Timestamp.Add(time.Duration(i) * inserter.Diff)
		times[i] = ts
	}
	return times
}

func (inserter *Inserter) generateSeries() map[string][]fields.Number {
	series := make(map[string][]fields.Number, inserter.Items)
	for i := 0; i < inserter.Items; i++ {
		itemID := fmt.Sprintf("bucket_item_%d", i)
		series[itemID] = inserter.generateDataPoints()
	}
	return series
}

func (inserter *Inserter) generateDataPoints() []fields.Number {
	rand.Seed(seed)
	dataPoints := make([]fields.Number, 0, inserter.Size)
	for i := 1; i <= inserter.Size; i++ {
		var val float64
		switch {
		case inserter.Sin:
			x := math.Remainder(float64(i), inserter.SinFreq) / float64(inserter.SinFreq)
			val = defaultScale * (math.Sin(2*math.Pi*x) + inserter.randRatio*rand.NormFloat64())
		default:
			val = defaultScale * rand.NormFloat64()
		}
		dataPoints = append(dataPoints, fields.Number(val))
	}
	return dataPoints
}

func (inserter *Inserter) insert(ctx context.Context, df views.DataFrame) (err error) {
	for i := 0; i < attempts; i++ {
		if _, err = inserter.Client.Insert(df).Do(ctx); err == nil {
			return
		}
		log.Printf("%d/%d Failed to insert: %v\nTrying again in %v", i, attempts, err, defaultSleepSeconds)
		time.Sleep(defaultSleepSeconds * time.Second)
	}
	return err
}
