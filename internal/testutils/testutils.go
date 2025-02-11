package testutils

import (
	"testing"

	"go.uber.org/goleak"
)

// VerifyTestMain contains any setup and teardown code that should be run
// before/after running all tests that belong to a package.
func VerifyTestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
