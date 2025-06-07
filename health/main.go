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
	health_checker "github.com/MaxiOtero6/TP-Distribuidos/health/src"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

const NODE_TYPE = "HEALTH"

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("health")
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

	formatStr := `%{time:2006-01-02 15:04:05} %{level:.5s}     ` + `%{message}`

	format := logging.MustStringFormatter(formatStr)
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

func initHealthChecker(v *viper.Viper) *health_checker.HealthChecker {
	id := v.GetString("id")

	clusterConfig := &model.WorkerClusterConfig{
		FilterCount:   v.GetInt("filter.count"),
		OverviewCount: v.GetInt("overview.count"),
		MapCount:      v.GetInt("map.count"),
		JoinCount:     v.GetInt("join.count"),
		ReduceCount:   v.GetInt("reduce.count"),
		MergeCount:    v.GetInt("merge.count"),
		TopCount:      v.GetInt("top.count"),
	}

	rabbitConfig := &model.RabbitConfig{
		FilterExchange:     v.GetString("consts.filterExchange"),
		OverviewExchange:   v.GetString("consts.overviewExchange"),
		MapExchange:        v.GetString("consts.mapExchange"),
		JoinExchange:       v.GetString("consts.joinExchange"),
		ReduceExchange:     v.GetString("consts.reduceExchange"),
		MergeExchange:      v.GetString("consts.mergeExchange"),
		TopExchange:        v.GetString("consts.topExchange"),
		ResultExchange:     v.GetString("consts.resultExchange"),
		EofExchange:        v.GetString("consts.eofExchange"),
		BroadcastID:        v.GetString("consts.broadcastId"),
		EofBroadcastRK:     v.GetString("consts.eofBroadcastRK"),
		ControlExchange:    v.GetString("consts.controlExchange"),
		ControlBroadcastRK: v.GetString("consts.controlBroadcastRK"),
		LeaderRK:           v.GetString("consts.leaderRK"),
		HealthExchange:     v.GetString("consts.healthExchange"),
	}

	infraConfig := model.NewInfraConfig(id, clusterConfig, rabbitConfig, "")

	log.Debugf("InfraConfig:\n\tWorkersConfig:%v\n\tRabbitConfig:%v", infraConfig.GetWorkers(), infraConfig.GetRabbit())

	exchanges, queues, _, err := utils.GetRabbitConfig(NODE_TYPE, v)

	healthQName := "health_" + fmt.Sprintf("%s_%s_queue", NODE_TYPE, id)

	queues = append(queues, map[string]string{
		"name": healthQName,
	})

	binds := make([]map[string]string, 0)
	binds = append(binds, map[string]string{
		"queue":    queues[0]["name"],
		"exchange": infraConfig.GetControlExchange(),
		"extraRK":  infraConfig.GetLeaderRK(),
	})

	binds = append(binds, map[string]string{
		"queue":    healthQName,
		"exchange": infraConfig.GetHealthExchange(),
	})

	if err != nil {
		log.Panicf("Failed to parse RabbitMQ configuration: %s", err)
	}

	hc := health_checker.NewHealthChecker(
		id,
		v.GetInt("healthCheckInterval"),
		infraConfig,
		queues[0]["name"],
		v.GetUint32("healthMaxStatus"),
	)
	hc.InitConfig(exchanges, queues, binds)

	log.Infof("Server '%v' ready", hc.ID)

	return hc
}

func handleSigterm(signalChan chan os.Signal, stoppable *health_checker.HealthChecker, wg *sync.WaitGroup) {
	defer wg.Done()
	s := <-signalChan

	if s != nil {
		stoppable.Stop()
		log.Infof("action: exit | result: success | signal: %v",
			s.String(),
		)
	}
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

	hc := initHealthChecker(v)

	wg.Add(1)
	go handleSigterm(signalChan, hc, &wg)

	hc.Run()

	close(signalChan)

	wg.Wait()

}
