package migrate

import (
	"testing"
)

func TestMigrator_FindMigratedDependencies(t *testing.T) {
	cases := []struct {
		name  string
		files string
	}{
		{
			name: "happy case",
			files: `

`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

		})
	}
}

func TestMigrator_UpdateImports(t *testing.T) {
	cases := []struct {
		name string
	}{
		{
			name: "happy case",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

		})
	}
}
