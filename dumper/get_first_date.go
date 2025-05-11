package dumper

import (
	"encoding/csv"
	"os"
	"time"

	"github.com/pkg/errors"
)

func (d *Dumper) getFirstWrittenDate() (time.Time, error) {
	file, err := os.OpenFile(d.fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return time.Time{}, nil
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// skip header
	if _, err = reader.Read(); err != nil {
		return time.Time{}, errors.Wrap(err, "failed to read text header")
	}
	firstDate, _, err := d.readData(reader)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "failed to read first date")
	}
	return firstDate, nil
}
