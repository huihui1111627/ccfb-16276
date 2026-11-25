package buffer

import (
	"strconv"
	"strings"

	"catalogbuffer/internal/disk"
)

func parsePageID(s string) (disk.PageID, error) {
	s = strings.TrimSpace(s)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return -1, err
	}
	return disk.PageID(n), nil
}
