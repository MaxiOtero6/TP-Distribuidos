package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	library "github.com/MaxiOtero6/TP-Distribuidos/client/src/client-library"
	"github.com/MaxiOtero6/TP-Distribuidos/client/src/client-library/model"
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

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("id")
	v.BindEnv("server", "address")
	v.BindEnv("log", "level")

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
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

func handleSigterm(signalChan chan os.Signal, clientLibrary *library.Library) {
	s := <-signalChan

	clientLibrary.Stop()

	log.Infof("action: exit | result: success | signal: %v",
		s.String(),
	)
}

func logResults(results *model.Results) {
	log.Infof("Query1 results:")
	for _, movie := range results.Query1 {
		log.Infof("MovieId: %s | Title: %s | Genres: %v", movie.MovieId, movie.Title, movie.Genres)
	}

	log.Infof("Query2 results:")
	for _, country := range results.Query2 {
		log.Infof("Country: %s | TotalInvestment: %d", country.Country, country.TotalInvestment)
	}

	log.Infof("Query3 results:")
	for kind, data := range results.Query3 {
		log.Infof("Kind %v; Movie: %s | AvgRating: %.2f", kind, data.Title, data.AvgRating)
	}

	log.Infof("Query4 results:")
	for _, actor := range results.Query4 {
		log.Infof("ActorId: %s | ActorName: %s", actor.ActorId, actor.ActorName)
	}

	log.Infof("Query5 results:")
	for movie, data := range results.Query5 {
		log.Infof("Movie: %s | RevenueBudgetRatio: %.2f", movie, data.RevenueBudgetRatio)
	}
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

	clientConfig := library.ClientConfig{
		ServerAddress:   v.GetString("server.address"),
		MaxAmount:       v.GetInt("batch.maxAmount"),
		MoviesFilePath:  v.GetString("data.movies"),
		RatingsFilePath: v.GetString("data.ratings"),
		CreditsFilePath: v.GetString("data.credits"),
	}

	clientLibrary, err := library.NewLibrary(clientConfig)
	if err != nil {
		log.Criticalf("%s", err)
	}

	go handleSigterm(signalChan, clientLibrary)

	results := clientLibrary.ProcessData()

	log.Infof("action: processData | result: success")

	logResults(results)

	clientLibrary.Stop()

}
