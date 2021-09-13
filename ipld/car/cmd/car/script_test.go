package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/rogpeppe/go-internal/testscript"
)

func TestMain(m *testing.M) {
	os.Exit(testscript.RunMain(m, map[string]func() int{
		"car": main1,
	}))
}

var update = flag.Bool("u", false, "update testscript output files")

func TestScript(t *testing.T) {
	t.Parallel()
	testscript.Run(t, testscript.Params{
		Dir: filepath.Join("testdata", "script"),
		Setup: func(env *testscript.Env) error {
			wd, err := os.Getwd()
			if err != nil {
				return err
			}
			env.Setenv("INPUTS", filepath.Join(wd, "testdata", "inputs"))
			return nil
		},
		UpdateScripts: *update,
	})
}
