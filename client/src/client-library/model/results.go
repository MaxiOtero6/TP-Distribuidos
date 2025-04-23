package model

type Query1 struct {
	MovieId string
	Title   string
	Genres  []string
}

type Query2 struct {
	Country         string
	TotalInvestment float64
}

type Query3 struct {
	AvgRating float32
	Title     string
}

type Query4 struct {
	ActorId   string
	ActorName string
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
