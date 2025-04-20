package server

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/model"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/utils"
	"google.golang.org/protobuf/proto"
)

func (s *Server) sendMoviesRabbit(movies []*model.Movie) {
	alphaTasks := utils.GetAlphaStageTask(movies)
	muTasks := utils.GetMuStageTask(movies)

	s.publishTasksRabbit(alphaTasks, s.infraConfig.Rabbit.FilterExchange)
	s.publishTasksRabbit(muTasks, s.infraConfig.Rabbit.OverviewExchange)
}

func (s *Server) sendRatingsRabbit(ratings []*model.Rating) {
	zetaTasks := utils.GetZetaStageRatingsTask(ratings, s.infraConfig.Workers.JoinCount)
	s.publishTasksRabbit(zetaTasks, s.infraConfig.Rabbit.JoinExchange)
}

func (s *Server) sendActorsRabbit(actors []*model.Actor) {
	iotaTasks := utils.GetIotaStageCreditsTask(actors, s.infraConfig.Workers.JoinCount)
	s.publishTasksRabbit(iotaTasks, s.infraConfig.Rabbit.JoinExchange)
}

func (s *Server) publishTasksRabbit(tasks map[string]*protocol.Task, exchange string) {
	for routingKey, task := range tasks {
		bytes, err := proto.Marshal(task)

		if err != nil {
			log.Errorf("Error marshalling %T task: %v", task.GetStage(), err)
			continue
		}

		s.rabbitMQ.Publish(exchange, routingKey, bytes)
	}
}
