package main

import (
	"flag"
	"github.com/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
	"path"
	"strings"
	"time"
)

const (
	DefaultEnvironment = "development"
	ZkLockPrefix       = "/monitor"
)

func main() {
	parseFlags()

	lock := acquireZooKeeperLock()

	defer func() {
		err := lock.Unlock()

		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Panic("Failed to release ZooKeeper lock")
		}
	}()
}

var config struct {
	zkServers   string
	environment string
}

func parseFlags() {
	flag.StringVar(&config.zkServers, "zk", "", "The addresses of the ZooKeeper servers to connect to")

	flag.StringVar(&config.environment, "env", DefaultEnvironment, "The 'environment' the monitor is running in")

	flag.Parse()
}

func acquireZooKeeperLock() *zk.Lock {
	addresses := strings.Split(config.zkServers, ",")
	log.WithFields(log.Fields{
		"addresses": addresses,
	}).Info("ZooKeeper server addresses")

	log.WithFields(log.Fields{
		"addresses":       addresses,
		"session timeout": 10 * time.Second,
	}).Info("Connecting to ZooKeeper")
	conn, _, err := zk.Connect(addresses, 10*time.Second)

	if err != nil {
		log.WithFields(log.Fields{
			"addresses": addresses,
			"error":     err,
		}).Panic("Failed to connect to ZooKeeper")
	}

	lock := zk.NewLock(conn, path.Join(ZkLockPrefix, config.environment), zk.WorldACL(zk.PermAll))

	err = lock.Lock()

	if err != nil {
		log.WithFields(log.Fields{
			"addresses": addresses,
			"error":     err,
		}).Panic("Failed to acquire ZooKeeper lock")
	}

	log.WithFields(log.Fields{
		"lock": lock,
	}).Info("Acquired ZooKeeper lock")

	return lock
}
