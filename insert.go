package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"os"
	"time"

	clarify "github.com/clarify/clarify-go"
	"github.com/clarify/clarify-go/fields"
	"github.com/clarify/clarify-go/views"
	"github.com/joho/godotenv"
)

type Creds struct {
	Url, IntegrationID, Password string
}

var counter = 0

const defaultVal = -999.0

func main() {
	log.Println("Starting")
	server := flag.String("server", "local", "choose environment: local, scratch1, dev. Default:local")
	method := flag.String("method", "continuous", "choose between 'continuous' and 'single'. Default:continuous")
	item := flag.String("item", "evaluator", "item to which write data")
	period := flag.Int("period", 15, "period of inserting data into clarify (in seconds). Default:15")
	val := flag.Float64("value", defaultVal, "choose value for inserting. Works only with 'single' method. Random for 'continuous'")
	diff := flag.Int("diff", 0, "minutes before now. Default:0")
	flag.Parse()

	err := godotenv.Load(*server + ".env")
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	defer ctx.Done()

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
	client := c.Client(ctx)
	publish_item(ctx, client, &creds, *item)

	switch {
	case *method == "continuous":
		ticker := time.NewTicker(time.Duration(*period) * time.Second)
		for {
			select {
			case <-ctx.Done():
				log.Println("context was closed")
			case tick := <-ticker.C:
				if *val == defaultVal {
					*val = 100 * rand.Float64()
				}
				t1 := fields.AsTimestamp(tick)

				if err := insert(ctx, client, t1, *item, *val); err != nil {
					time.Sleep(5 * time.Second)
					continue
				}
				counter++
			}
		}

	case *method == "single":
		t1 := fields.AsTimestamp(time.Now().Add(-time.Duration(*diff) * time.Minute))
		if err := insert(ctx, client, t1, *item, *val); err != nil {
			panic(err)
		}

	default:
		panic("method not in [single, continuous]")
	}
}

func publish_item(ctx context.Context, client *clarify.Client, creds *Creds, item string) error {
	selectResult, err := client.SelectSignals(creds.IntegrationID).Include("item").Limit(100).Do(ctx)
	if err != nil {
		return err
	}

	newItems := make(map[string]views.ItemSave, len(selectResult.Data))
	for _, signal := range selectResult.Data {
		if signal.Attributes.Name == item {
			newItems[signal.ID] = views.PublishedItem(signal)
			result, err := client.PublishSignals(creds.IntegrationID, newItems).Do(ctx)
			if err != nil {
				return err
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			enc.Encode(result)
			return nil
		}
	}
	log.Printf("Item %s was not found under integration %s", item, creds.IntegrationID)
	return nil
}

func insert(ctx context.Context, client *clarify.Client, ts fields.Timestamp, item string, val float64) error {
	df := views.DataFrame{
		item: {ts: val},
	}
	_, err := client.Insert(df).Do(ctx)
	if err != nil {
		log.Printf("Failed to insert data. Error: %s\nSleeping for 5 seconds\n", err)
		return err
	}

	log.Printf("Inserted %.2f at %s\n", val, ts.Time().String())
	return nil
}
