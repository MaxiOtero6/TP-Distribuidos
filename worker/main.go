package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	worker "github.com/MaxiOtero6/TP-Distribuidos/worker/src"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the WORKER_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("worker")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("id")
	v.BindEnv("log", "level")

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
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

func initWorker(v *viper.Viper, signalChan chan os.Signal) *worker.Worker {
	workerType := v.GetString("type")

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

	infraConfig := model.NewInfraConfig(clusterConfig, rabbitConfig)

	log.Debugf("InfraConfig:\n\tWorkersConfig:%v\n\tRabbitConfig:%v", infraConfig.GetWorkers(), infraConfig.GetRabbit())

	exchanges, queues, binds, err := utils.GetRabbitConfig(workerType, v)

	if err != nil {
		log.Panicf("Failed to parse RabbitMQ configuration: %s", err)
	}

	w := worker.NewWorker(v.GetString("id"), workerType, infraConfig, signalChan)
	w.InitConfig(exchanges, queues, binds)

	log.Infof("Worker %v ready", w.WorkerId)

	return w
}

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM)

	v, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
	}

	w := initWorker(v, signalChan)
	w.Run()

	close(signalChan)
}
