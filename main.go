package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strings"

	"github.com/clarify/clarify-go"
	"github.com/joho/godotenv"
	"github.com/sarzhann/clarify-inserter/pkg"
)

const defaultCredsPath = ".env"

var credsFlag = flag.String("credentials", "", "relative path to .env file with Clarify.io credentials")

func main() {
	ctx := context.Background()
	defer ctx.Done()

	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		slog.Error("Please specify a subcommand.")
		return
	}
	cmd, args := args[0], args[1:]

	var credsPath string
	switch {
	case *credsFlag != "":
		if !strings.HasSuffix(*credsFlag, ".env") {
			slog.Error("flag -credentials should be a relative path to .env file with Clarify.io credentials")
			return
		}
		credsPath = *credsFlag
	case os.Getenv("CLARIFY_CREDENTIALS") != "":
		credsPath = os.Getenv("CLARIFY_CREDENTIALS")
	default:
		credsPath = defaultCredsPath
	}

	err := godotenv.Load(credsPath)
	if err != nil {
		slog.Error("Error loading .env file", "error", err)
		return
	}
	creds := pkg.LoadCreds()
	client := clarify.Credentials{
		APIURL:      creds.Url,
		Integration: creds.IntegrationID,
		Credentials: clarify.CredentialsAuth{
			Type:         clarify.TypeBasicAuth,
			ClientID:     creds.IntegrationID,
			ClientSecret: creds.Password,
		},
	}.Client(ctx)

	var exec func(context.Context) error
	switch cmd {
	case "insert":
		inserter, err := pkg.NewInserter(client, args)
		if err != nil {
			slog.Error("Error creating inserter", "error", err)
			return
		}
		exec = inserter.Exec()
	default:
		slog.Error("Unknown subcommand", "subcommand", cmd)
		return
	}

	if err := exec(ctx); err != nil {
		slog.Error("Error executing command", "error", err)
		return
	}
}
