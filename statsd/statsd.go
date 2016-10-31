package statsd

import (
	"net"
	"sync"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
)

const (
	// UDPMaxPacketSize means UDP packet limit, see https://en.wikipedia.org/wiki/User_Datagram_Protocol#Packet_structure
	UDPMaxPacketSize int = 64 * 1024

	// AllowedPendingMessages is the number of UDP messages allowed to queue up, once filled, the statsd server will start dropping packets
	AllowedPendingMessages = 10000
)

// NewStatsd XXX
func NewStatsd(conf *config.Config) *Statsd {
	reporter := NewReporter(conf)
	return &Statsd{
		conf:     conf,
		reporter: reporter,
		in:       make(chan []byte, AllowedPendingMessages),
	}
}

// Statsd XXX
type Statsd struct {
	conf     *config.Config
	reporter *Reporter

	// Channel for all incoming statsd packets
	in chan []byte

	// drops tracks the number of dropped metrics.
	drops int
}

// Run XXX
func (s *Statsd) Run(shutdown chan struct{}) error {
	var wg sync.WaitGroup
	interval := 30 * time.Second

	// channel shared between all Plugin threads for collecting metrics
	metricC := make(chan metric.Metric, 10000)

	wg.Add(3)
	go func() {
		defer wg.Done()
		if err := s.listen(shutdown); err != nil {
			log.Error(err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := s.reporter.Run(shutdown, metricC, interval); err != nil {
			log.Errorf("Reporter routine failed, exiting: %s", err.Error())
			close(shutdown)
		}
	}()

	go func() {
		defer wg.Done()
		if err := s.parser(shutdown, metricC, interval); err != nil {
			log.Error(err)
		}
	}()

	wg.Wait()
	close(s.in)
	return nil
}

func (s *Statsd) listen(shutdown chan struct{}) error {
	addr, err := net.ResolveUDPAddr("udp", s.conf.GetStatsdAddr())
	if err != nil {
		log.Fatalln("Can't resolve address:", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalln("Error listening:", err)
	}

	log.Infoln("Statsd listening on:", addr)

	for {
		select {
		case <-shutdown:
			log.Infof("Statsd server thread exit")
			if err := conn.Close(); err != nil {
				return err
			}
			return nil
		default:
			s.handleClient(conn)
		}
	}
}

func (s *Statsd) handleClient(conn *net.UDPConn) {
	buf := make([]byte, UDPMaxPacketSize)
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		log.Infoln("failed to read UDP msg because of ", err.Error())
		return
	}

	bufCopy := make([]byte, n)
	copy(bufCopy, buf[:n])

	select {
	case s.in <- bufCopy:
	default:
		s.drops++
		if s.drops == 1 || s.drops%AllowedPendingMessages == 0 {
			log.Infof("ERROR: statsd message queue full. "+
				"We have dropped %d messages so far. ", s.drops)
		}
	}
}

// parser monitors the s.in channel, if there is a packet ready, it parses the
// packet into statsd strings and then calls parseStatsdLine, which parses a
// single statsd metric.
func (s *Statsd) parser(shutdown chan struct{}, metricC chan metric.Metric, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	agg := NewAggregator(metricC, s.conf)

	var packet []byte
	for {
		select {
		case <-shutdown:
			return nil
		case <-ticker.C:
			agg.Flush()
		case packet = <-s.in:
			log.Debugf("Received packet: %s", string(packet))
			agg.SubmitPackets(string(packet))
		}
	}
}
