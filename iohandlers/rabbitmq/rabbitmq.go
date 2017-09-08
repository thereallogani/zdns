package rabbitmq

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/thereallogani/zdns"
	"gopkg.in/yaml.v2"
)

var errMsg string = "this iohandler requires a config file with a username, password, ip address, qname, and qsize"

type rabbitmqConfig struct {
	User     string `yaml:"Rabbitmq-Username"`
	Pwd      string `yaml:"Rabbitmq-Password"`
	Ip       string `yaml:"Rabbitmq-IP"`
	Qname    string `yaml:"Rabbitmq-Qname"`
	Qsize    int64  `yaml:"Rabbitmq-Qsize"`
	Tls      bool   `yaml:"Rabbitmq-Tls"`
	Certs    string `yaml:"Rabbitmq-Certs"`
	CAServer string `yaml:"Rabbitmq-CertificateAuthorityServer"`
}

func (c *rabbitmqConfig) readConfig(filepath string) *rabbitmqConfig {
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		log.Fatal(errMsg, err.Error())
	}
	err = yaml.Unmarshal(data, c)
	if err != nil {
		log.Fatal("Data in rabbitmq config wasn't formatted correctly", err.Error())
	}
	return c
}

func (c *rabbitmqConfig) getTlsConfig() *tls.Config {
	tlsCfg := new(tls.Config)
	tlsCfg.RootCAs = x509.NewCertPool()
	tlsCfg.ServerName = c.CAServer
	if ca, err := ioutil.ReadFile(c.Certs + "cacert.pem"); err == nil {
		tlsCfg.RootCAs.AppendCertsFromPEM(ca)
	} else {
		log.Fatal("cacert.pem in certs/ directory is required", err.Error())
	}

	if cert, err := tls.LoadX509KeyPair(c.Certs+"cert.pem", c.Certs+"key.pem"); err == nil {
		tlsCfg.Certificates = append(tlsCfg.Certificates, cert)
	} else {
		log.Fatal("cert.pem and key.pem required in certs directory", err.Error())
	}
	return tlsCfg
}

func (c *rabbitmqConfig) getConnection() (*amqp.Connection, error) {
	if c.Tls != true {
		return amqp.Dial("amqp://" + c.User + ":" + c.Pwd + "@" + c.Ip)
	} else {
		tlsCfg := c.getTlsConfig()
		return amqp.DialTLS("amqps://"+c.User+":"+c.Pwd+"@"+c.Ip, tlsCfg)
	}
}

type InputHandler struct {
	channel *amqp.Channel
	conn    *amqp.Connection
	domains <-chan amqp.Delivery
	q       amqp.Queue
}

func (h *InputHandler) Initialize(conf *zdns.GlobalConf) {
	var rabbitConf rabbitmqConfig
	rabbitConf.readConfig(conf.InputHandlerConfig)

	conn, err := rabbitConf.getConnection()
	if err != nil {
		log.Fatal("Failed to connect to specified rabbitmq server! ", err.Error())
	}
	h.conn = conn

	ch, err := h.conn.Channel()
	if err != nil {
		log.Fatal("Failed to create a rabbitmq channel! ", err.Error())
	}
	h.channel = ch

	args := make(amqp.Table)
	args["x-max-length"] = rabbitConf.Qsize

	q, err := h.channel.QueueDeclare(
		rabbitConf.Qname, // Queue name
		true,             // Durable
		false,            // Delete when unused
		false,            // Exclusive
		false,            // No-wait
		args,             // Arguements
	)
	if err != nil {
		log.Fatal("Failed to declare a queue! ", err.Error())
	}
	h.q = q
	h.channel.Qos(
		300,   // Prefetch count
		0,     // Prefectch size
		false, // Global
	)

	domains, err := h.channel.Consume(
		h.q.Name, // Queue
		"",       // Consumer
		false,    // Auto-ack
		false,    // Exclusive
		false,    // No-local
		false,    // No-wait
		nil,      // Args
	)
	if err != nil {
		log.Fatal("Failed to consume from channel! ", err.Error())
	}
	h.domains = domains
}

func (h *InputHandler) FeedChannel(in chan<- interface{}, wg *sync.WaitGroup, zonefileInput bool) error {
	for delivery := range h.domains {
		delivery.Ack(false) // acknowledge the delivery
		split := strings.Fields(string(delivery.Body))
		for _, domain := range split {
			in <- domain
		}
	}
	close(in)
	h.conn.Close()
	(*wg).Done()
	return nil
}

// register rabbitmq input handler
func init() {
	in := new(InputHandler)
	zdns.RegisterInputHandler("rabbitmq", in)
}
