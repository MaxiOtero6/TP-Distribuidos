package server

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server-comm/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/model"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/utils"
)


func (s *Server) processMovieBatch(batch *protocol.Batch) ([][]*model.Movie, error) {
	var movies [][]*model.Movie

	for _, row := range batch.Data {

		fields := utils.ParseLine(&row.Data)
		if fields == nil {
			return nil, fmt.Errorf("Invalid row data")
		}

		movie := utils.ParseMovie(fields)
		if movie == nil {
			return nil, fmt.Errorf("Invalid movie data")
		}
		movies = append(movies, movie)

	}
	return movies, nil
}

func (s *Server) processCreditBatch(batch *protocol.Batch) ([][]*model.Actor, error) {
	var actors [][]*model.Actor

	for _, row := range batch.Data {

		fields := utils.ParseLine(&row.Data)
		if fields == nil {
			return nil, fmt.Errorf("Invalid row data")
		}

		actor := utils.ParseCredit(fields)
		if actor == nil {
			return nil, fmt.Errorf("Invalid actor data")
		}
		actors = append(actors, actor)

	}
	return actors, nil
}

func (s *Server) processRatingsBatch(batch *protocol.Batch) ([][]*model.Rating, error) {
	var ratings [][]*model.Rating

	for _, row := range batch.Data {

		fields := utils.ParseLine(&row.Data)
		if fields == nil {
			return nil, fmt.Errorf("Invalid row data")
		}

		rating := utils.ParseRating(fields)
		if rating == nil {
			return nil, fmt.Errorf("Invalid rating data")
		}
		ratings = append(ratings, rating)

	}
	return ratings, nil
}

func (s *Server) validateBatchType(batchMessage *protocol.Batch) error {
	if batchMessage.Type != protocol.FileType_MOVIES &&
		batchMessage.Type != protocol.FileType_CREDITS &&
		batchMessage.Type != protocol.FileType_RATINGS {
		return fmt.Errorf("Invalid file type: %v", batchMessage.Type)
	}
	return nil
}