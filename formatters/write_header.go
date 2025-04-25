package formatters

import (
	"fmt"
	"github.com/pkg/errors"
	"log"
	"os"
)

func WriteHeader(file *os.File, dataType string) {
	var err error
	switch dataType {
	case "klines":
		_, err = fmt.Fprintln(file, "<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>")
	case "trades":
		_, err = fmt.Fprintln(file, "<DATE>,<TIME>,<MSEC>,<TRADENO>,<LAST>,<BID>,<BIDQTY>,<ASK>,<ASKQTY>,<OPER>")
	}
	if err != nil {
		log.Fatalln(errors.Wrapf(err, "failed to write header to file %s", file.Name()))
	}
}
