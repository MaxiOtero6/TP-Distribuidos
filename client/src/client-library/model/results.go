package model

type Query1 struct {
	MovieId string   `json:"-"`
	Title   string   `json:"title"`
	Genres  []string `json:"genres"`
}

type Query2 struct {
	Country         string
	TotalInvestment uint64
}

type Query3 struct {
	AvgRating float32
	Title     string
}

type Query4 struct {
	ActorId        string `json:"-"`
	ActorName      string `json:"name"`
	Participations uint64 `json:"count"`
}

type Query5 struct {
	RevenueBudgetRatio float32
}

type Results struct {
	Query1 []*Query1
	Query2 []*Query2
	Query3 map[string]*Query3
	Query4 []*Query4
	Query5 map[string]*Query5
}
