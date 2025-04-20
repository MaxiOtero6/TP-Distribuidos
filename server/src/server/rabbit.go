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

	s.publishTasksRabbit(alphaTasks, s.infraConfig.GetFilterExchange())
	s.publishTasksRabbit(muTasks, s.infraConfig.GetOverviewExchange())
}

func (s *Server) sendRatingsRabbit(ratings []*model.Rating) {
	zetaTasks := utils.GetZetaStageRatingsTask(ratings, s.infraConfig.GetJoinCount())
	s.publishTasksRabbit(zetaTasks, s.infraConfig.GetJoinExchange())
}

func (s *Server) sendActorsRabbit(actors []*model.Actor) {
	iotaTasks := utils.GetIotaStageCreditsTask(actors, s.infraConfig.GetJoinCount())
	s.publishTasksRabbit(iotaTasks, s.infraConfig.GetJoinExchange())
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
