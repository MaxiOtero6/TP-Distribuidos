package utils

import (
	"testing"

	"github.com/MaxiOtero6/TP-Distribuidos/server/src/model"
	"github.com/stretchr/testify/assert"
)

func TestGetAlphaStageTask(t *testing.T) {
	movies := []*model.Movie{
		{Id: "1", Title: "Movie 1", ProdCountries: []string{"USA"}, Genres: []string{"Action"}, ReleaseYear: 2020},
		{Id: "2", Title: "Movie 2", ProdCountries: []string{"UK"}, Genres: []string{"Drama"}, ReleaseYear: 2019},
	}

	tasks := GetAlphaStageTask(movies)

	assert.Len(t, tasks, 1)
	assert.Contains(t, tasks, "")
	assert.Len(t, tasks[""].GetAlpha().Data, 2)
	assert.Equal(t, "Movie 1", tasks[""].GetAlpha().Data[0].Title)
	assert.Equal(t, "Movie 2", tasks[""].GetAlpha().Data[1].Title)
}

func TestGetZetaStageRatingsTask(t *testing.T) {
	ratings := []*model.Rating{
		{MovieId: "1", Rating: 4.5},
		{MovieId: "2", Rating: 3.8},
		{MovieId: "3", Rating: 5.0},
	}

	tasks := GetZetaStageRatingsTask(ratings, 2)

	assert.Len(t, tasks, 2) // Since joinersCount is 2, tasks are distributed into 2 groups
	for _, task := range tasks {
		assert.NotNil(t, task.GetZeta())
	}
}

func TestGetIotaStageCreditsTask(t *testing.T) {
	actors := []*model.Actor{
		{Id: "1", Name: "Actor 1", MovieId: "1"},
		{Id: "2", Name: "Actor 2", MovieId: "2"},
		{Id: "3", Name: "Actor 3", MovieId: "1"},
	}

	tasks := GetIotaStageCreditsTask(actors, 2)

	assert.Len(t, tasks, 2) // Since joinersCount is 2, tasks are distributed into 2 groups
	for _, task := range tasks {
		assert.NotNil(t, task.GetIota())
	}
}

func TestGetMuStageTask(t *testing.T) {
	movies := []*model.Movie{
		{Id: "1", Title: "Movie 1", Revenue: 1000000, Budget: 500000, Overview: "Overview 1"},
		{Id: "2", Title: "Movie 2", Revenue: 2000000, Budget: 1000000, Overview: "Overview 2"},
	}

	tasks := GetMuStageTask(movies)

	assert.Len(t, tasks, 1)
	assert.Contains(t, tasks, "")
	assert.Len(t, tasks[""].GetMu().Data, 2)
	assert.Equal(t, "Movie 1", tasks[""].GetMu().Data[0].Title)
	assert.Equal(t, "Movie 2", tasks[""].GetMu().Data[1].Title)
}
