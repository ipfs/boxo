package files

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModePermsToUnixPerms(t *testing.T) {
	assert.Equal(t, uint32(0777), ModePermsToUnixPerms(os.FileMode(0777)))
	assert.Equal(t, uint32(04755), ModePermsToUnixPerms(0755|os.ModeSetuid))
	assert.Equal(t, uint32(02777), ModePermsToUnixPerms(0777|os.ModeSetgid))
	assert.Equal(t, uint32(01377), ModePermsToUnixPerms(0377|os.ModeSticky))
	assert.Equal(t, uint32(05300), ModePermsToUnixPerms(0300|os.ModeSetuid|os.ModeSticky))
}

func TestUnixPermsToModePerms(t *testing.T) {
	assert.Equal(t, os.FileMode(0777), UnixPermsToModePerms(0777))
	assert.Equal(t, 0755|os.ModeSetuid, UnixPermsToModePerms(04755))
	assert.Equal(t, 0777|os.ModeSetgid, UnixPermsToModePerms(02777))
	assert.Equal(t, 0377|os.ModeSticky, UnixPermsToModePerms(01377))
	assert.Equal(t, 0300|os.ModeSetuid|os.ModeSticky, UnixPermsToModePerms(05300))
}
