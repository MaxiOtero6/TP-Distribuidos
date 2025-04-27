package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/client_handler"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/server"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

const NODE_TYPE = "SERVER"

type Instance interface {
	Run() error
	Stop()
}

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("server")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("port")
	v.BindEnv("log", "level")

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead\n")
	}

	rabbitConfig := viper.New()
	rabbitConfig.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	rabbitConfig.SetConfigFile("./rabbitConfig.yaml")
	if err := rabbitConfig.ReadInConfig(); err != nil {
		fmt.Printf("Rabbit configuration could not be read.\n")
	}

	// Merge the rabbit-specific config with the main config
	if err := v.MergeConfigMap(rabbitConfig.AllSettings()); err != nil {
		return nil, fmt.Errorf("failed to merge rabbit-specific config: %w", err)
	}

	return v, nil
}

// InitLogger Receives the log level to be set in go-logging as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	// Set the backends to be used.
	logging.SetBackend(backendLeveled)
	return nil
}

func handleSigterm(signalChan chan os.Signal, stoppable Instance, wg *sync.WaitGroup) {
	defer wg.Done()
	s := <-signalChan

	if s != nil {
		stoppable.Stop()
		log.Infof("action: exit | result: success | signal: %v",
			s.String(),
		)
	}
}

func initClientHandler(v *viper.Viper) *client_handler.ClientHandler {
	const SOCKET_FD uintptr = 3

	id := v.GetString("id")

	clusterConfig := &model.WorkerClusterConfig{
		FilterCount:   v.GetInt("filter.count"),
		OverviewCount: v.GetInt("overview.count"),
		MapCount:      v.GetInt("map.count"),
		JoinCount:     v.GetInt("join.count"),
		ReduceCount:   v.GetInt("reduce.count"),
		TopCount:      v.GetInt("top.count"),
	}

	rabbitConfig := &model.RabbitConfig{
		FilterExchange:   v.GetString("consts.filterExchange"),
		OverviewExchange: v.GetString("consts.overviewExchange"),
		MapExchange:      v.GetString("consts.mapExchange"),
		JoinExchange:     v.GetString("consts.joinExchange"),
		ReduceExchange:   v.GetString("consts.reduceExchange"),
		TopExchange:      v.GetString("consts.topExchange"),
		ResultExchange:   v.GetString("consts.resultExchange"),
		BroadcastID:      v.GetString("consts.broadcastId"),
	}

	infraConfig := model.NewInfraConfig(id, clusterConfig, rabbitConfig)

	log.Debugf("InfraConfig:\n\tWorkersConfig:%v\n\tRabbitConfig:%v", infraConfig.GetWorkers(), infraConfig.GetRabbit())

	exchanges, queues, binds, err := utils.GetRabbitConfig(NODE_TYPE, v)

	if err != nil {
		log.Panicf("Failed to parse RabbitMQ configuration: %s", err)
	}

	c, err := client_handler.NewClienthandler(id, infraConfig, SOCKET_FD)

	if err != nil {
		log.Panicf("Failed to initialize client_handler: %s", err)
	}

	c.InitConfig(exchanges, queues, binds)

	log.Infof("ClientHandler ready for server '%v'", c.ServerID)

	return c
}

func initServer(v *viper.Viper) *server.Server {
	address := v.GetString("address")
	id := v.GetString("id")

	s, err := server.NewServer(id, address)

	if err != nil {
		log.Panicf("Failed to initialize server: %s", err)
	}

	log.Infof("Server '%v' ready", s.ID)

	return s
}

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM)

	var wg sync.WaitGroup

	v, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
	}

	var instance Instance

	if len(os.Args) == 1 {
		instance = initServer(v)
	} else if len(os.Args) == 2 {
		instance = initClientHandler(v)
	} else {
		log.Panicf("Bad call to server. Usage for server: %s ; Usage for clientHandler: %s child", os.Args[0], os.Args[0])
	}

	wg.Add(1)
	go handleSigterm(signalChan, instance, &wg)

	instance.Run()

	close(signalChan)

	wg.Wait()

}
