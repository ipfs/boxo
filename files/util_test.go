package files

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModePermsToUnixPerms(t *testing.T) {
	assert.Equal(t, uint32(0o777), ModePermsToUnixPerms(os.FileMode(0o777)))
	assert.Equal(t, uint32(0o4755), ModePermsToUnixPerms(0o755|os.ModeSetuid))
	assert.Equal(t, uint32(0o2777), ModePermsToUnixPerms(0o777|os.ModeSetgid))
	assert.Equal(t, uint32(0o1377), ModePermsToUnixPerms(0o377|os.ModeSticky))
	assert.Equal(t, uint32(0o5300), ModePermsToUnixPerms(0o300|os.ModeSetuid|os.ModeSticky))
}

func TestUnixPermsToModePerms(t *testing.T) {
	assert.Equal(t, os.FileMode(0o777), UnixPermsToModePerms(0o777))
	assert.Equal(t, 0o755|os.ModeSetuid, UnixPermsToModePerms(0o4755))
	assert.Equal(t, 0o777|os.ModeSetgid, UnixPermsToModePerms(0o2777))
	assert.Equal(t, 0o377|os.ModeSticky, UnixPermsToModePerms(0o1377))
	assert.Equal(t, 0o300|os.ModeSetuid|os.ModeSticky, UnixPermsToModePerms(0o5300))
}
