//go:build !windows

package locationimport

import "os"

func atomicReplace(source string, destination string) error {
	return os.Rename(source, destination)
}
