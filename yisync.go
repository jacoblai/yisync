package main

import (
	"flag"
	"fmt"
	"github.com/jacoblai/yisync/engine"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func main() {
	var (
		masterCon    = flag.String("mc", "", "master mongodb connect string flag")
		masterDbName = flag.String("md", "", "master database name") //主数据库名
		clientCon    = flag.String("cc", "", "client mongodb connect string flag")
		clientDbName = flag.String("cd", "", "client database name") //从数据库名
	)
	flag.Parse()

	if *masterCon == "" || *masterDbName == "" || *clientCon == "" || *clientDbName == "" {
		log.Fatal("flags error")
	}

	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}

	eng := engine.NewDbEngine(dir)

	if _, err := os.Stat(dir + "/resumeToken"); err == nil {
		bts, err := ioutil.ReadFile(dir + "/resumeToken")
		if err != nil {
			log.Println("read nil resume token")
		} else {
			eng.ResumeToken = bts
		}
	}

	err = eng.Open(*masterCon, *masterDbName, *clientCon, *clientDbName)
	if err != nil {
		panic(err)
	}

	go eng.Sync()

	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for range signalChan {
			eng.Close()
			fmt.Println("safe exit")
			cleanupDone <- true
		}
	}()
	<-cleanupDone
}
