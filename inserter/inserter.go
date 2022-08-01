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
	server       string
	method       string
	item         string
	value        float64
	itemsCount   int
	signalsCount int
	bucket       bool
	frequency    time.Duration
	diff         time.Duration
}

func New(ctx context.Context) (*Inserter, error) {
	server := flag.String("server", "local", "choose environment variable name. Stored under envs/<server>.env")
	method := flag.String("method", "stream", "choose between 'stream', 'single'. Default:'stream'")
	item := flag.String("item", "data", "item to which write data. Default:'data'")
	value := flag.Float64("value", -999.0, "choose value for inserting. Works only with 'single' method. Random for 'stream'")
	itemsCount := flag.Int("items", 10, "Number of items per insert (Applicable only if 'bucket' is enabled")
	signalsCount := flag.Int("signals", 10, "Number of signals per item per insert (Applicable only if 'bucket' is enabled")
	bucket := flag.Bool("bucket", false, "to insert a bucket of data")
	frequency := flag.Int("frequency", 15, "period of inserting data into clarify (in seconds). Applicable only for 'stream' method. Default:15")
	diff := flag.Int("diff", 0, "minutes before now. Default:0")
	flag.Parse()

	err := godotenv.Load("envs/" + *server + ".env")
	if err != nil {
		return nil, err
	}

	creds := Creds{
		Url:           os.Getenv("APIURL"),
		IntegrationID: os.Getenv("CLARIFY_INTEGRATION_ID"),
		Password:      os.Getenv("CLARIFY_PASSWORD"),
	}

	c := clarify.Credentials{
		APIURL:      creds.Url,
		Integration: creds.IntegrationID,
		Credentials: clarify.CredentialsAuth{
			Type:         clarify.TypeBasicAuth,
			ClientID:     creds.IntegrationID,
			ClientSecret: creds.Password,
		},
	}

	return &Inserter{
		c:            c.Client(ctx),
		server:       *server,
		method:       *method,
		item:         *item,
		value:        *value,
		itemsCount:   *itemsCount,
		signalsCount: *signalsCount,
		bucket:       *bucket,
		frequency:    time.Duration(*frequency),
		diff:         time.Duration(*diff),
	}, nil
}

func (inserter *Inserter) Run(ctx context.Context) {
	log.Printf("Start inserter")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	switch {
	case inserter.method == "single":
		ts := fields.AsTimestamp(time.Now().Add(-inserter.diff * time.Minute))
		if err := inserter.insertSingle(ctx, ts); err != nil {
			log.Fatalf("Failed to insert data: %v", err)
		}

	case inserter.method == "stream":
		ticker := time.NewTicker(inserter.frequency * time.Second)
		for {
			select {
			case <-ctx.Done():
				log.Println("context was closed")
			case tick := <-ticker.C:
				if inserter.bucket {
					if err := inserter.insertBucket(ctx); err != nil {
						log.Printf("Failed to insert data: %v", err)
						time.Sleep(5 * time.Second)
						continue
					}
				} else {
					ts := fields.AsTimestamp(tick)
					inserter.value = 100 * (rand.NormFloat64()/2 + math.Sin((float64(tick.Second()) / (math.Pi * math.Pi))))
					if err := inserter.insertSingle(ctx, ts); err != nil {
						log.Printf("Failed to insert data: %v", err)
						time.Sleep(5 * time.Second)
						continue
					}
				}

			}
		}
	default:
		log.Fatal("method not in [single, stream]")
	}
	log.Printf("Finish inserter")
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
