package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	migrate "github.com/ipfs/boxo/cmd/boxo-migrate/internal"
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
					&cli.BoolFlag{
						Name:  "force",
						Usage: "run even if no .git folder is found",
					},
				},
				Action: func(clictx *cli.Context) error {
					dryrun := clictx.Bool("dryrun")
					force := clictx.Bool("force")
					configFile := clictx.String("config")

					migrator, err := buildMigrator(dryrun, configFile)
					if err != nil {
						return err
					}

					fmt.Printf("\n\n")

					if !force {
						p, err := os.Getwd()
						if err != nil {
							return fmt.Errorf("failed to fetch current working directory: %w", err)
						}

						for {
							g := filepath.Join(p, ".git")
							_, err := os.Stat(g)
							if err == nil {
								break
							}
							newP := filepath.Dir(p)
							if p == newP {
								return fmt.Errorf(`
⚠️ Version Control System Check ⚠️

We couldn't locate a .git folder in any parent paths. We strongly recommend
using a Version Control System to help you easily compare and revert to a
previous state if needed, as this tool doesn't have an undo feature.

If you're using a different VCS or like to live dangerously, you can bypass this
check by adding the --force flag.`)
							}
							p = newP
						}
					}

					if !dryrun {
						err := migrator.GoGet("github.com/ipfs/boxo@v0.8.0")
						if err != nil {
							return err
						}
					}

					if err := migrator.UpdateImports(); err != nil {
						return err
					}

					if dryrun {
						return nil
					}

					if err := migrator.GoModTidy(); err != nil {
						return err
					}

					fmt.Printf("Your code has been successfully updated. Note that you might still need to manually fix up parts of your code.\n\n")
					fmt.Printf("You should also consider running the 'boxo-migrate check-dependencies' command to see if you have any other dependencies on migrated code.\n\n")

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
							"You still have dependencies on repos which have migrated to Boxo.",
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
