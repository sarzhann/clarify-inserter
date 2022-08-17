package inserter

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/clarify/clarify-go"
	"github.com/clarify/clarify-go/fields"
	"github.com/clarify/clarify-go/views"
	"github.com/joho/godotenv"
)

type Creds struct {
	Url, IntegrationID, Password string
}

type Inserter struct {
	c            *clarify.Client
	frequency    time.Duration
	diff         time.Duration
	value        float64
	server       string
	method       string
	item         string
	itemsCount   int
	signalsCount int
	bucket       bool
	sinWave      bool
}

func New(ctx context.Context) (*Inserter, error) {
	var ins Inserter
	flag.DurationVar(&ins.frequency, "frequency", 15*time.Second, "period of inserting data into clarify (in seconds). Applicable only for 'stream' method. Default:15")
	flag.DurationVar(&ins.diff, "diff", 0*time.Minute, "minutes before now. Default:0")
	flag.Float64Var(&ins.value, "value", -999.0, "choose value for inserting. Works only with 'single' method. Random for 'stream'")
	flag.StringVar(&ins.server, "server", "local", "choose environment variable name. Stored under envs/<server>.env")
	flag.StringVar(&ins.method, "method", "stream", "choose between 'stream', 'single'. Default:'stream'")
	flag.StringVar(&ins.item, "item", "data", "item to which write data. Default:'data'")
	flag.IntVar(&ins.itemsCount, "items", 10, "Number of items per insert (Applicable only if 'bucket' is enabled")
	flag.IntVar(&ins.signalsCount, "signals", 10, "Number of signals per item per insert (Applicable only if 'bucket' is enabled")
	flag.BoolVar(&ins.bucket, "bucket", false, "to insert a bucket of data")
	flag.BoolVar(&ins.sinWave, "sinWave", false, "To use sinus wave pattern for insertion")
	flag.Parse()

	err := godotenv.Load("envs/" + ins.server + ".env")
	if err != nil {
		return nil, err
	}
	creds := Creds{
		Url:           os.Getenv("APIURL"),
		IntegrationID: os.Getenv("CLARIFY_INTEGRATION_ID"),
		Password:      os.Getenv("CLARIFY_PASSWORD"),
	}

	ins.c = clarify.Credentials{
		APIURL:      creds.Url,
		Integration: creds.IntegrationID,
		Credentials: clarify.CredentialsAuth{
			Type:         clarify.TypeBasicAuth,
			ClientID:     creds.IntegrationID,
			ClientSecret: creds.Password,
		},
	}.Client(ctx)

	return &ins, nil
}

func sleep(err error) {
	log.Printf("Failed to insert data: %v", err)
	time.Sleep(5 * time.Second)
}

func (inserter *Inserter) Run(ctx context.Context) {
	log.Printf("Starting inserter...")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	switch {
	case inserter.method == "single":
		var err error
		attempts := 5
		ts := fields.AsTimestamp(time.Now().Add(-inserter.diff))
		for i := 0; i < attempts; i++ {
			err = inserter.insertSingle(ctx, ts)
			if err == nil {
				return
			}
			sleep(err)
		}

	case inserter.method == "stream":
		ticker := time.NewTicker(inserter.frequency)
		for {
			select {
			case <-ctx.Done():
				log.Println("context was closed")
			case tick := <-ticker.C:
				if inserter.bucket {
					if err := inserter.insertBucket(ctx); err != nil {
						sleep(err)
						continue
					}
				} else {
					ts := fields.AsTimestamp(tick)
					if inserter.sinWave {
						inserter.value = 100 * (rand.NormFloat64()/2 + math.Sin((float64(tick.Second()) / (math.Pi * math.Pi))))
					} else {
						inserter.value = 100 * rand.Float64()
					}
					if err := inserter.insertSingle(ctx, ts); err != nil {
						sleep(err)
						continue
					}
				}
			}
		}
	default:
		log.Fatal("method not in [single, stream]")
	}
}

func (inserter *Inserter) insertSingle(ctx context.Context, ts fields.Timestamp) error {
	df := views.DataFrame{
		inserter.item: {
			ts: inserter.value,
		},
	}

	_, err := inserter.c.Insert(df).Do(ctx)
	if err != nil {
		return err
	}

	log.Printf("Inserted %.2f at %s\n", inserter.value, ts.Time().String())
	return nil
}

func (inserter *Inserter) insertBucket(ctx context.Context) error {
	times := inserter.generateTimes()
	series := inserter.generateSeries()

	dfRaw := views.RawDataFrame{
		Times:  times,
		Series: series,
	}
	df := dfRaw.DataFrame()

	_, err := inserter.c.Insert(df).Do(ctx)
	if err != nil {
		return err
	}

	log.Printf("Server: %s; diff: %d; period: %d; dfRaw: %d", inserter.server, inserter.diff, inserter.frequency, len(dfRaw.Times)*len(dfRaw.Series))
	return nil
}

func (inserter *Inserter) generateTimes() []fields.Timestamp {
	times := make([]fields.Timestamp, inserter.signalsCount)

	for i := 0; i < inserter.signalsCount; i++ {
		ts := fields.AsTimestamp(time.Now().Add(-inserter.diff * time.Hour).Add(time.Duration(i) * time.Minute))
		times[i] = ts
	}

	return times
}

func (inserter *Inserter) generateSeries() map[string][]fields.Number {
	series := make(map[string][]fields.Number, inserter.itemsCount)

	for i := 0; i < inserter.itemsCount; i++ {
		series[fmt.Sprintf("item_new_%d", i)] = inserter.generateSignals()
	}

	return series
}

func (inserter *Inserter) generateSignals() []fields.Number {
	signals := make([]fields.Number, inserter.signalsCount)

	for i := 0; i < inserter.signalsCount; i++ {
		signals[i] = fields.Number(rand.Float64())
	}

	return signals
}
