package main

import (
	"context"
	"flag"
	"time"

	"github.com/clarify/clarify-go"
	"github.com/joho/godotenv"
	"github.com/sarzhann/clarify-inserter/pkg"
)

func main() {
	ctx := context.Background()
	defer ctx.Done()

	var server string
	flag.StringVar(&server, "server", "local", "environment variable name stored under envs/<server>.env")

	var inserter pkg.Inserter
	flag.StringVar(&inserter.Method, "method", "stream", "choose between 'stream', 'single'.")
	flag.StringVar(&inserter.SignalID, "signal_id", "data", "id of the signal to write data.")
	flag.DurationVar(&inserter.Diff, "diff", time.Minute, "time difference between points. Minute by default")
	flag.DurationVar(&inserter.Frequency, "frequency", 5*time.Second, "period of inserting data into clarify. Applicable only for 'stream' method.")
	flag.Float64Var(&inserter.Value, "value", pkg.DefaultValue, "value for inserting. Works only with 'single' method. Random for 'stream' if not set")
	flag.BoolVar(&inserter.Sin, "sin", false, "use sinus wave pattern for insert values (only for 'stream' method)")
	flag.Float64Var(&inserter.SinFreq, "sin_freq", 0, "Frequency of sinus wave")
	flag.IntVar(&inserter.Items, "items", 5, "number of items per insert (only for 'bucket' method")
	flag.IntVar(&inserter.Size, "size", 10, "the size of series per item (only for 'bucket' method")

	flag.Parse()

	err := godotenv.Load("envs/" + server + ".env")
	if err != nil {
		panic(err)
	}
	creds := pkg.LoadCreds()
	inserter.Client = clarify.Credentials{
		APIURL:      creds.Url,
		Integration: creds.IntegrationID,
		Credentials: clarify.CredentialsAuth{
			Type:         clarify.TypeBasicAuth,
			ClientID:     creds.IntegrationID,
			ClientSecret: creds.Password,
		},
	}.Client(ctx)

	inserter.Run(ctx)
}
