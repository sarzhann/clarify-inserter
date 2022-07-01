package pkg

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"time"

	"github.com/clarify/clarify-go"
	"github.com/clarify/clarify-go/fields"
	"github.com/clarify/clarify-go/views"
)

const (
	retrySleep         = 5 * time.Second
	defaultRetries     = 2
	defaultConcurrency = 5
	isConcurrent       = true
	truncateTimeStart  = true

	defaultValue        = -999.9
	defaultSineWaveFreq = 60
	sineScale           = 100
)

const (
	methodStream = "stream"
	methodBucket = "bucket"
	methodSingle = "single"
)

type inserter struct {
	client *clarify.Client
	fs     *flag.FlagSet

	frequency    time.Duration
	sampleRate   time.Duration
	timeStart    fields.Timestamp
	method       string
	signalID     string
	value        float64
	sineWaveFreq float64
	itemCount    int
	dataPoints   int
	bulkCount    int
	sineWave     bool

	bulkDataPoints int
	randRatio      float64
}

func NewInserter(c *clarify.Client, args []string) (*inserter, error) {
	ins := &inserter{
		client: c,
		fs:     flag.NewFlagSet("insert", flag.ExitOnError),
	}
	ins.fs.StringVar(&ins.method, "method", methodStream, "choose between 'stream', 'single', 'bucket'.")
	ins.fs.StringVar(&ins.signalID, "signal_id", "inserter_item", "id of the signal to write data.")
	ins.fs.DurationVar(&ins.sampleRate, "sample-rate", time.Minute, "time difference between points. Minute by default")
	ins.fs.DurationVar(&ins.frequency, "frequency", 5*time.Second, "period of inserting data into clarify. Applicable only for 'stream' method.")
	ins.fs.Float64Var(&ins.value, "value", defaultValue, "value for inserting. Works only with 'single' method. Random for 'stream' if not set")
	ins.fs.Float64Var(&ins.sineWaveFreq, "sin_freq", 0, "frequency of the sine wave")
	ins.fs.BoolVar(&ins.sineWave, "sin", false, "use sine wave pattern for insert values (only for 'stream' method)")
	ins.fs.IntVar(&ins.itemCount, "items", 1, "number of items per insert (only for 'bucket' method")
	ins.fs.IntVar(&ins.dataPoints, "data-points", 100, "the size of series per item (only for 'bucket' method")
	ins.fs.IntVar(&ins.bulkCount, "bulks", 10, "the number of bulk insert (only for 'bucket' method")
	ins.fs.Parse(args)

	if err := ins.validate(); err != nil {
		return nil, err
	}

	return ins, nil
}

func (ins *inserter) validate() error {
	switch ins.method {
	case methodBucket, methodStream, methodSingle:
	default:
		return fmt.Errorf("method must be on of: %s, %s, %s", methodBucket, methodStream, methodSingle)
	}
	if ins.itemCount <= 0 {
		return fmt.Errorf("items must be positive integer")
	}
	if ins.dataPoints <= 0 {
		return fmt.Errorf("data points must be positive integer")
	}
	if ins.frequency <= 0 {
		return fmt.Errorf("frequency must be positive duration")
	}
	if ins.sampleRate <= 0 {
		return fmt.Errorf("sample rate must be positive duration")
	}
	if ins.bulkCount <= 0 {
		return fmt.Errorf("bulk size must be positive integer")
	}

	if ins.sineWave {
		switch {
		case ins.sineWaveFreq == 0:
			switch ins.method {
			case methodBucket:
				ins.sineWaveFreq = float64(ins.dataPoints) / math.Sqrt(float64(ins.dataPoints))
			case methodStream:
				ins.sineWaveFreq = defaultSineWaveFreq
			}
		case ins.sineWaveFreq < 0:
			return fmt.Errorf("sine wave frequency must be positive")
		}
		ins.randRatio = 1 / math.Sqrt(ins.sineWaveFreq)
	}

	ins.bulkDataPoints = ins.dataPoints / ins.bulkCount

	now := time.Now()
	if truncateTimeStart {
		now = now.Truncate(24 * time.Hour)
	}
	ins.timeStart = fields.AsTimestamp(now)
	return nil
}

func (ins *inserter) Exec() func(ctx context.Context) error {
	switch ins.method {
	case methodSingle:
		return ins.Single
	case methodBucket:
		return ins.Bucket
	case methodStream:
		return ins.Stream
	default:
		return nil
	}
}

func (ins *inserter) do(ctx context.Context, df views.DataFrame) (err error) {
	for i := 0; i < defaultRetries; i++ {
		if _, err = ins.client.Insert(df).Do(ctx); err == nil {
			return
		}
		slog.Error("Failed to insert", "error", err, "retry", i)
		time.Sleep(retrySleep)
	}
	return err
}

// ====================== Stream ======================

func (ins *inserter) Stream(ctx context.Context) (err error) {
	ticker := time.NewTicker(ins.frequency)
	var counter int
	for {
		select {
		case <-ctx.Done():
			slog.Info("context was closed")
		case <-ticker.C:
			val := ins.value
			switch {
			case ins.sineWave:
				x := math.Remainder(float64(counter), ins.sineWaveFreq) / float64(ins.sineWaveFreq)
				val = sineScale * (math.Sin(2*math.Pi*x) + ins.randRatio*rand.NormFloat64())
			case ins.value == defaultValue:
				val = sineScale * rand.NormFloat64()
			}

			ts := ins.timeStart.Add(time.Duration(counter) * ins.sampleRate)
			if err = ins.insertDataPoint(ctx, ts, val); err != nil {
				return
			}
			counter++
		}
	}
}

// ====================== Single ======================

func (ins *inserter) Single(ctx context.Context) (err error) {
	return ins.do(ctx, views.DataFrame{ins.signalID: {ins.timeStart: ins.value}})
}

func (ins *inserter) insertDataPoint(ctx context.Context, ts fields.Timestamp, val float64) (err error) {
	df := views.DataFrame{ins.signalID: {ts: val}}
	if err = ins.do(ctx, df); err == nil {
		slog.Info("Inserted",
			"value", val,
			"timestamp", ts.Time(),
		)
	}
	return
}

// ====================== Bucket ======================

func (ins *inserter) Bucket(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	times := ins.generateTimes()
	series := ins.generateSeries()

	b := bulk{
		errCh:     make(chan error, ins.bulkCount),
		semaphore: make(chan struct{}, defaultConcurrency),
	}
	defer close(b.errCh)
	defer close(b.semaphore)

	for i := 0; i < ins.bulkCount; i++ {
		start := i * ins.bulkDataPoints
		end := start + ins.bulkDataPoints
		if end > ins.dataPoints {
			end = ins.dataPoints
		}

		df := rawDataFrame{
			times:  times[start:end],
			series: make(map[string][]float64, ins.itemCount),
		}
		for itemID, dataPoints := range series {
			df.series[itemID] = dataPoints[start:end]
		}

		if isConcurrent {
			go ins.insertBulk(ctx, i, &b, df)
		} else {
			ins.insertBulk(ctx, i, &b, df)
		}
	}

	for i := 0; i < ins.bulkCount; i++ {
		if err := <-b.errCh; err != nil {
			slog.Error("Error inserting bulk", "error", err, "bulk", i)
			cancel()
			return fmt.Errorf("error inserting bulk: %v", err)
		}
	}

	return nil
}

type bulk struct {
	errCh     chan error
	semaphore chan struct{}
}

type rawDataFrame struct {
	times  []fields.Timestamp
	series map[string][]float64
}

func (ins *inserter) insertBulk(ctx context.Context, idx int, b *bulk, raw rawDataFrame) {
	select {
	case <-ctx.Done():
		return
	default:
		b.semaphore <- struct{}{}
		defer func() { <-b.semaphore }()

		fmt.Println("inserting bulk", idx)
		df := views.DataFrame{}
		for itemID, dataPoints := range raw.series {
			df[itemID] = make(views.DataSeries, ins.dataPoints)
			for i, ts := range raw.times {
				df[itemID][ts] = dataPoints[i]
			}
		}
		b.errCh <- ins.do(ctx, df)
		fmt.Println("inserted bulk", idx)
	}
}

func (ins *inserter) generateTimes() []fields.Timestamp {
	times := make([]fields.Timestamp, ins.dataPoints)
	for i := 0; i < ins.dataPoints; i++ {
		times[i] = ins.timeStart.Add(time.Duration(i) * ins.sampleRate)
	}
	return times
}

func (ins *inserter) generateSeries() map[string][]float64 {
	series := make(map[string][]float64, ins.itemCount)

	dp := ins.generateDataPoints()
	switch ins.itemCount {
	case 1:
		series[ins.signalID] = dp
	default:
		for i := 0; i < ins.itemCount; i++ {
			series[fmt.Sprintf("%s_%d", ins.signalID, i)] = dp
		}
	}
	return series
}

func (ins *inserter) generateDataPoints() []float64 {
	dataPoints := make([]float64, 0, ins.dataPoints)
	for i := 1; i <= ins.dataPoints; i++ {
		val := ins.value
		switch {
		case ins.sineWave:
			x := math.Remainder(float64(i), ins.sineWaveFreq) / float64(ins.sineWaveFreq)
			val = sineScale * (math.Sin(2*math.Pi*x) + ins.randRatio*rand.NormFloat64())
		case ins.value == defaultValue:
			val = sineScale * rand.NormFloat64()
		}
		dataPoints = append(dataPoints, val)
	}
	return dataPoints
}
