package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	migrate "github.com/ipfs/boxo/cmd/migrate/internal"
	"github.com/urfave/cli/v2"
)

func loadConfig(configFile string) (migrate.Config, error) {
	if configFile != "" {
		f, err := os.Open(configFile)
		if err != nil {
			return migrate.Config{}, fmt.Errorf("opening config file: %w", err)
		}
		defer f.Close()
		return migrate.ReadConfig(f)
	}
	return migrate.DefaultConfig, nil
}

func buildMigrator(dryrun bool, configFile string) (*migrate.Migrator, error) {
	config, err := loadConfig(configFile)
	if err != nil {
		return nil, err
	}
	dir, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("getting working dir: %w", err)
	}
	return &migrate.Migrator{
		DryRun: dryrun,
		Dir:    dir,
		Config: config,
	}, nil
}

func main() {
	app := &cli.App{
		Name: "migrate",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "config",
				Usage: "a JSON config file",
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "update-imports",
				Usage: "rewrites imports of the current module for go-libipfs repos",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name: "dryrun",
					},
				},
				Action: func(clictx *cli.Context) error {
					dryrun := clictx.Bool("dryrun")
					configFile := clictx.String("config")

					migrator, err := buildMigrator(dryrun, configFile)
					if err != nil {
						return err
					}
					if err := migrator.UpdateImports(); err != nil {
						return err
					}

					return nil
				},
			},
			{
				Name:  "check-dependencies",
				Usage: "checks the current module for dependencies that have migrated to go-libipfs",
				Action: func(clictx *cli.Context) error {
					configFile := clictx.String("config")

					migrator, err := buildMigrator(false, configFile)
					if err != nil {
						return err
					}

					deps, err := migrator.FindMigratedDependencies()
					if err != nil {
						return err
					}
					if len(deps) > 0 {
						fmt.Println(strings.Join([]string{
							"You still have dependencies on repos which have migrated to go-libipfs.",
							"You should consider not having these dependencies to avoid multiple versions of the same code.",
							"You can use 'go mod why' or 'go mod graph' to find the reason for these dependencies.",
							"",
							"Dependent module versions:",
							"",
							strings.Join(deps, "\n"),
						}, "\n"))
					}
					return nil
				},
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
