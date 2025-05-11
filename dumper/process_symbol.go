package dumper

import (
	"context"
	"log"
	"os"
	"time"
)

func (d *Dumper) ProcessSymbol(ctx context.Context) interface{} {
	firstDate, err := d.getFirstWrittenDate()
	if err != nil {
		log.Fatalf("Error getting first date: %v", err)
	}

	var lastDate time.Time
	currentDate := d.startDate
	var lastTradeID int64
	if !firstDate.IsZero() && firstDate.Before(d.startDate) && firstDate.AddDate(0, 0, 1).Before(d.startDate) {
		log.Println("First date is before start date. Removed:", d.fileName)
		os.Remove(d.fileName)
	} else if lastDate, lastTradeID, err = d.getLastData(d.startDate); err != nil {
		if !os.IsNotExist(err) {
			log.Println("File is in wrong format. Removed:", d.fileName)
			os.Remove(d.fileName)
		}
	}
	if !lastDate.IsZero() {
		currentDate = lastDate.Truncate(24 * time.Hour)
	}

	for ; !currentDate.After(d.endDate); currentDate = currentDate.AddDate(0, 0, 1) {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if lastDate, lastTradeID, err = d.DumpData(ctx, currentDate, lastDate, lastTradeID); err != nil {
			log.Printf("Error fetching daily data for %s[%v]: %v", d.symbol, currentDate, err)
		}
	}

	return nil
}
