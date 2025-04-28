package dumper

import (
	"context"
	"log"
	"os"
)

func (d *Dumper) ProcessSymbol(ctx context.Context) interface{} {
	firstDate, err := d.getFirstWrittenDate()
	if err != nil {
		log.Fatalf("Error getting first date: %v", err)
	}

	lastDate := d.startDate
	if !firstDate.IsZero() && firstDate.Before(d.startDate) && firstDate.AddDate(0, 0, 1).Before(d.startDate) {
		log.Println("First date is before start date. Removed:", d.fileName)
		os.Remove(d.fileName)
	} else if lastDate, err = d.getLastDate(d.startDate); err != nil {
		if !os.IsNotExist(err) {
			log.Println("File is in wrong format. Removed:", d.fileName)
			os.Remove(d.fileName)
		}
		lastDate = d.startDate
	}

	for ; !lastDate.After(d.endDate); lastDate = lastDate.AddDate(0, 1, 0) {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err = d.DumpData(ctx, lastDate, true); err != nil {
			lastDate, err = d.getLastDate(lastDate)
			if err != nil {
				log.Println("Error getting last date:", err)
				lastDate = d.startDate
			}
			log.Printf("Error fetching monthly data for %s[%v]: %v", d.symbol, lastDate, err)
			for ; !lastDate.After(d.endDate); lastDate = lastDate.AddDate(0, 0, 1) {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				if err = d.DumpData(ctx, lastDate, false); err != nil {
					log.Printf("Error fetching daily data for %s[%v]: %v", d.symbol, lastDate, err)
				}
			}
		}
	}
	return nil
}
