package utils

import (
	"github.com/MaxiOtero6/TP-Distribuidos/client/src/client-library/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
)

var QUERIES_AMOUNT int = 1

// ResultParser is a struct that handles the parsing of results from the server
// and stores them in a Results struct.
// It also keeps track of the number of EOF messages received.
type ResultParser struct {
	results  model.Results
	eofCount int
}

// NewResultParser creates a new ResultParser instance
// and initializes the results field with empty slices/maps for each query type.
func NewResultParser() *ResultParser {
	return &ResultParser{
		results: model.Results{
			Query1: make([]*model.Query1, 0),
			Query2: make([]*model.Query2, 0),
			Query3: make(map[string]*model.Query3, 0),
			Query4: make([]*model.Query4, 0),
			Query5: make(map[string]*model.Query5, 0),
		},
		eofCount: 0,
	}
}

// IsDone checks if the ResultParser has received the expected number of EOF messages.
// If it has, it means that all results have been received and processed.
func (p *ResultParser) IsDone() bool {
	return p.eofCount >= QUERIES_AMOUNT
}

// GetResults returns the parsed results if the ResultParser is done processing.
func (p *ResultParser) GetResults() *model.Results {
	if !p.IsDone() {
		return nil
	}

	return &p.results
}

// handleResult1 processes the Result1 message from the server.
// It extracts the movie ID, title, and genres from the message
// and appends them to the Query1 slice in the results struct.
func (p *ResultParser) handleResult1(result *protocol.Result1) {
	for _, data := range result.GetData() {
		query1 := &model.Query1{
			MovieId: data.GetId(),
			Title:   data.GetTitle(),
			Genres:  data.GetGenres(),
		}

		p.results.Query1 = append(p.results.Query1, query1)
	}
}

// handleResult2 processes the Result2 message from the server.
// It extracts the country and total investment from the message
// and appends them to the Query2 slice in the results struct.
// It also checks if the number of countries returned is within the expected range (up to 5).
// If the number of countries exceeds 5, an error is logged.
// If the Query2 slice already contains data, an error is logged.
func (p *ResultParser) handleResult2(result *protocol.Result2) {
	allData := result.GetData()

	// Less countries can be returned, but not more than 5
	// This is because the server sends a batch of more than 5 countries, but the client can
	// receive less than 5 countries if in the processing of the batch
	// the server has less than 5 countries
	if len(allData) > 5 {
		log.Errorf("action: handleResult2 | result: fail | error: expected up to 5 elements, got %d", len(allData))
		return
	}

	if len(p.results.Query2) != 0 {
		log.Errorf("action: handleResult2 | result: fail | error: Query2 already exists")
		return
	}

	for _, data := range allData {
		query2 := &model.Query2{
			Country:         data.GetCountry(),
			TotalInvestment: data.GetTotalInvestment(),
		}

		p.results.Query2 = append(p.results.Query2, query2)
	}
}

// handleResult3 processes the Result3 message from the server.
// It extracts the maximum and minimum average ratings and titles from the message
// and stores them in the Query3 map in the results struct.
// It checks if the maximum and minimum ratings already exist in the map.
// If they do, an error is logged.
// It also checks if the number of elements in the data slice is exactly 2.
// If not, an error is logged.
// The maximum and minimum ratings are stored in the map with keys "max" and "min".
func (p *ResultParser) handleResult3(result *protocol.Result3) {
	if _, ok := p.results.Query3["max"]; ok {
		log.Errorf("action: handleResult3 | result: fail | error: max already exists")
		return
	}

	if _, ok := p.results.Query3["min"]; ok {
		log.Errorf("action: handleResult3 | result: fail | error: min already exists")
		return
	}

	data := result.GetData()

	if len(data) != 2 {
		log.Errorf("action: handleResult3 | result: fail | error: expected 2 elements, got %d", len(data))
		return
	}

	max := &model.Query3{
		AvgRating: data[0].GetRating(),
		Title:     data[0].GetTitle(),
	}

	min := &model.Query3{
		AvgRating: data[1].GetRating(),
		Title:     data[1].GetTitle(),
	}

	p.results.Query3["max"] = max
	p.results.Query3["min"] = min
}

// handleResult4 processes the Result4 message from the server.
// It extracts the actor ID and name from the message
// and appends them to the Query4 slice in the results struct.
// It checks if the number of actors returned is within the expected range (up to 10).
// If the number of actors exceeds 10, an error is logged.
// If the Query4 slice already contains data, an error is logged.
// The actors are stored in the Query4 slice.
func (p *ResultParser) handleResult4(result *protocol.Result4) {
	allData := result.GetData()

	// Less actors can be returned, but not more than 10
	// This is because the server sends a batch of 10 actors, but the client can
	// receive less than 10 actors if the server has less than 10 actors
	if len(allData) > 10 {
		log.Errorf("action: handleResult4 | result: fail | error: expected up to 10 elements, got %d", len(allData))
		return
	}

	if len(p.results.Query4) != 0 {
		log.Errorf("action: handleResult4 | result: fail | error: Query4 already exists")
		return
	}

	for _, data := range allData {
		query4 := &model.Query4{
			ActorId:   data.GetActorId(),
			ActorName: data.GetActorName(),
		}
		p.results.Query4 = append(p.results.Query4, query4)
	}
}

// handleResult5 processes the Result5 message from the server.
// It extracts the revenue budget ratio from the message
// and stores it in the Query5 map in the results struct.
// It checks if the negative and positive keys already exist in the map.
// If they do, an error is logged.
func (p *ResultParser) handleResult5(result *protocol.Result5) {
	data := result.GetData()

	for _, d := range data {
		key := "negative"

		if d.GetSentiment() {
			key = "positive"
		}

		if _, ok := p.results.Query5[key]; !ok {
			p.results.Query5[key] = &model.Query5{
				RevenueBudgetRatio: 0,
			}
		}

		p.results.Query5[key].RevenueBudgetRatio += d.GetRatio()
	}
}

func (p *ResultParser) Save(results *protocol.ResultsResponse) {
	for _, result := range results.GetResults() {
		switch result.Message.(type) {
		case *protocol.ResultsResponse_Result_Result1:
			log.Debugf("action: Save | result: success | Result1 received")
			p.handleResult1(result.GetResult1())
		case *protocol.ResultsResponse_Result_Result2:
			log.Debugf("action: Save | result: success | Result2 received")
			p.handleResult2(result.GetResult2())
		case *protocol.ResultsResponse_Result_Result3:
			log.Debugf("action: Save | result: success | Result3 received")
			p.handleResult3(result.GetResult3())
		case *protocol.ResultsResponse_Result_Result4:
			log.Debugf("action: Save | result: success | Result4 received")
			p.handleResult4(result.GetResult4())
		case *protocol.ResultsResponse_Result_Result5:
			log.Debugf("action: Save | result: success | Result5 received")
			p.handleResult5(result.GetResult5())
		case *protocol.ResultsResponse_Result_OmegaEOF:
			p.eofCount++
			log.Debugf("action: Save | result: success | EOF received | eofCount: %d", p.eofCount)
		}
	}
}
