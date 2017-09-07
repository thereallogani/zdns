package file

import (
	"bufio"
	"os"
	"sync"

	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
	"github.com/thereallogani/zdns"
)

type InputHandler struct {
	filepath string
}

func (h *InputHandler) Initialize(conf *zdns.GlobalConf) {
	h.filepath = conf.InputFilePath
}

func (h *InputHandler) FeedChannel(in chan<- interface{}, wg *sync.WaitGroup, zonefileInput bool) error {
	var f *os.File
	if h.filepath == "" || h.filepath == "-" {
		f = os.Stdin
	} else {
		var err error
		f, err = os.Open(h.filepath)
		if err != nil {
			log.Fatal("unable to open input file:", err.Error())
		}
	}
	if zonefileInput {
		tokens := dns.ParseZone(f, ".", h.filepath)
		for t := range tokens {
			in <- t
		}
	} else {
		s := bufio.NewScanner(f)
		for s.Scan() {
			in <- s.Text()
		}
		if err := s.Err(); err != nil {
			log.Fatal("input unable to read file", err)
		}
	}
	close(in)
	(*wg).Done()
	return nil
}

type OutputHandler struct {
	filepath string
}

func (h *OutputHandler) Initialize(conf *zdns.GlobalConf) {
	h.filepath = conf.OutputFilePath
}

func (h *OutputHandler) WriteResults(results <-chan string, wg *sync.WaitGroup) error {
	var f *os.File
	if h.filepath == "" || h.filepath == "-" {
		f = os.Stdout
	} else {
		var err error
		f, err = os.OpenFile(h.filepath, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			log.Fatal("unable to open output file:", err.Error())
		}
		defer f.Close()
	}
	for n := range results {
		f.WriteString(n + "\n")
	}
	(*wg).Done()
	return nil
}

// register handlers
func init() {
	in := new(InputHandler)
	zdns.RegisterInputHandler("file", in)

	out := new(OutputHandler)
	zdns.RegisterOutputHandler("file", out)
}
