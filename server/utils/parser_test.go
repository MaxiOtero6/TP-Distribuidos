package utils

import (
	"testing"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
)

func compareSlicesOrdered(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}

	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}

	return true
}

func compareMatrixOrdered(m1, m2 [][]string) bool {
	if len(m1) != len(m2) {
		return false
	}

	for i := range m1 {
		if !compareSlicesOrdered(m1[i], m2[i]) {
			return false
		}
	}

	return true
}

func TestMapJsonRegex(t *testing.T) {
	t.Run("TestGetNamesFromJsonGenreObjectList", func(t *testing.T) {
		json := "[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"
		rx := `'name': '([^']+)'`

		expected := []string{"Animation", "Comedy", "Family"}
		actual := mapJsonRegex(json, rx)

		if !compareSlicesOrdered(expected, actual) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	t.Run("TestGetIdsFromJsonGenreObjectList", func(t *testing.T) {
		json := "[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"
		rx := `'id': (\d+)`

		expected := []string{"16", "35", "10751"}
		actual := mapJsonRegex(json, rx)

		if !compareSlicesOrdered(expected, actual) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	t.Run("TestGetNonExistentFieldFromJsonGenreObjectList", func(t *testing.T) {
		json := "[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"
		rx := `'nonExistentField': '([^']+)'`

		expected := []string{}
		actual := mapJsonRegex(json, rx)

		if !compareSlicesOrdered(expected, actual) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})
}

func TestMapJsonRegexTuple(t *testing.T) {
	t.Run("TestGetIdsAndNamesFromJsonGenreObjectList", func(t *testing.T) {
		json := "[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"
		rx := `'id': (\d+).*?'name': '([^']+)'`

		expected := [][]string{
			{"16", "Animation"},
			{"35", "Comedy"},
			{"10751", "Family"},
		}
		actual := mapJsonRegexTuple(json, rx, 2)

		if !compareMatrixOrdered(expected, actual) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	t.Run("TestGetIdsFromJsonGenreObjectList", func(t *testing.T) {
		json := "[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"
		rx := `'id': (\d+)`

		expected := [][]string{
			{"16"},
			{"35"},
			{"10751"},
		}
		actual := mapJsonRegexTuple(json, rx, 1)

		if !compareMatrixOrdered(expected, actual) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	t.Run("TestGetNonExistentFieldFromJsonGenreObjectList", func(t *testing.T) {
		json := "[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"
		rx := `'nonExistentField': '([^']+)'`

		expected := [][]string{}
		actual := mapJsonRegexTuple(json, rx, 1)

		if !compareMatrixOrdered(expected, actual) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	t.Run("TestGetZeroFieldsWithExistentFieldsRegexFromJsonGenreObjectList", func(t *testing.T) {
		json := "[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"
		rx := `'id': (\d+)`

		expected := [][]string{}
		actual := mapJsonRegexTuple(json, rx, 0)

		if !compareMatrixOrdered(expected, actual) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})
}

func TestParseLine(t *testing.T) {
	t.Run("TestParseLineWithComma", func(t *testing.T) {
		line := `1,"John, Doe",25`
		expected := []string{"1", "John, Doe", "25"}
		actual := parseLine(&line)

		if !compareSlicesOrdered(expected, actual) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	t.Run("TestParseLineWithQuotes", func(t *testing.T) {
		line := `1,"John Doe, a great person",25`
		expected := []string{"1", "John Doe, a great person", "25"}
		actual := parseLine(&line)

		if !compareSlicesOrdered(expected, actual) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	t.Run("TestParseLineWithEmptyFields", func(t *testing.T) {
		line := `1,"",`
		expected := []string{"1", "", ""}
		actual := parseLine(&line)

		if !compareSlicesOrdered(expected, actual) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	t.Run("TestParseLineWithJsonObjects", func(t *testing.T) {
		line := `1,"{'id': 16, 'name': 'Animation'}",25`
		expected := []string{"1", "{'id': 16, 'name': 'Animation'}", "25"}
		actual := parseLine(&line)

		if !compareSlicesOrdered(expected, actual) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	t.Run("asdas", func(t *testing.T) {
		line := `False,"{'id': 10194, 'name': 'Toy Story Collection', 'poster_path': '/7G9915LfUQ2lVfwMEEhDsn3kT4B.jpg', 'backdrop_path': '/9FBwqcd9IRruEDUrTdcaafOMKUq.jpg'}",30000000,"[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]",http://toystory.disney.com/toy-story,862,tt0114709,en,Toy Story,"Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody from their owner, the duo eventually learns to put aside their differences.",21.946943,/rhIRbceoE9lR4veEXuwCC2wARtG.jpg,"[{'name': 'Pixar Animation Studios', 'id': 3}]","[{'iso_3166_1': 'US', 'name': 'United States of America'}]",1995-10-30,373554033,81.0,"[{'iso_639_1': 'en', 'name': 'English'}]",Released,,Toy Story,False,7.7,5415`

		t.Errorf("%v", parseLine(&line))
	})
}

func compareMovie(t *testing.T, actual, expected *protocol.DataRow) {
	if actual == nil || expected == nil {
		t.Errorf("Expected non-nil values, but got actual: %v, expected: %v", actual, expected)
		return
	}

	if actual.Data.(*protocol.DataRow_Movie).Movie.Id != expected.Data.(*protocol.DataRow_Movie).Movie.Id {
		t.Errorf("Expected Id %s, but got %s",
			expected.Data.(*protocol.DataRow_Movie).Movie.Id,
			actual.Data.(*protocol.DataRow_Movie).Movie.Id,
		)
	}

	if actual.Data.(*protocol.DataRow_Movie).Movie.Title != expected.Data.(*protocol.DataRow_Movie).Movie.Title {
		t.Errorf("Expected Title %s, but got %s",
			expected.Data.(*protocol.DataRow_Movie).Movie.Title,
			actual.Data.(*protocol.DataRow_Movie).Movie.Title,
		)
	}

	if !compareSlicesOrdered(actual.Data.(*protocol.DataRow_Movie).Movie.ProdCountries, expected.Data.(*protocol.DataRow_Movie).Movie.ProdCountries) {
		t.Errorf("Expected ProdCountries %s, but got %s",
			expected.Data.(*protocol.DataRow_Movie).Movie.ProdCountries[0],
			actual.Data.(*protocol.DataRow_Movie).Movie.ProdCountries[0],
		)
	}

	if actual.Data.(*protocol.DataRow_Movie).Movie.Revenue != expected.Data.(*protocol.DataRow_Movie).Movie.Revenue {
		t.Errorf("Expected Revenue %d, but got %d",
			expected.Data.(*protocol.DataRow_Movie).Movie.Revenue,
			actual.Data.(*protocol.DataRow_Movie).Movie.Revenue,
		)
	}

	if actual.Data.(*protocol.DataRow_Movie).Movie.Budget != expected.Data.(*protocol.DataRow_Movie).Movie.Budget {
		t.Errorf("Expected Budget %d, but got %d",
			expected.Data.(*protocol.DataRow_Movie).Movie.Budget,
			actual.Data.(*protocol.DataRow_Movie).Movie.Budget,
		)
	}

	if actual.Data.(*protocol.DataRow_Movie).Movie.Overview != expected.Data.(*protocol.DataRow_Movie).Movie.Overview {
		t.Errorf("Expected Overview %s, but got %s",
			expected.Data.(*protocol.DataRow_Movie).Movie.Overview,
			actual.Data.(*protocol.DataRow_Movie).Movie.Overview,
		)
	}

	if actual.Data.(*protocol.DataRow_Movie).Movie.ReleaseDate != expected.Data.(*protocol.DataRow_Movie).Movie.ReleaseDate {
		t.Errorf("Expected ReleaseDate %s, but got %s",
			expected.Data.(*protocol.DataRow_Movie).Movie.ReleaseDate,
			actual.Data.(*protocol.DataRow_Movie).Movie.ReleaseDate,
		)
	}

	if !compareSlicesOrdered(actual.Data.(*protocol.DataRow_Movie).Movie.Genres, expected.Data.(*protocol.DataRow_Movie).Movie.Genres) {
		t.Errorf("Expected %d Genres, but got %d",
			len(expected.Data.(*protocol.DataRow_Movie).Movie.Genres),
			len(actual.Data.(*protocol.DataRow_Movie).Movie.Genres),
		)
	}
}

func TestParseMovie(t *testing.T) {

	line := `False,"{'id': 10194, 'name': 'Toy Story Collection', 'poster_path': '/7G9915LfUQ2lVfwMEEhDsn3kT4B.jpg', 'backdrop_path': '/9FBwqcd9IRruEDUrTdcaafOMKUq.jpg'}",30000000,"[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]",http://toystory.disney.com/toy-story,862,tt0114709,en,Toy Story,"Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody from their owner, the duo eventually learns to put aside their differences.",21.946943,/rhIRbceoE9lR4veEXuwCC2wARtG.jpg,"[{'name': 'Pixar Animation Studios', 'id': 3}]","[{'iso_3166_1': 'US', 'name': 'United States of America'}]",1995-10-30,373554033,81.0,"[{'iso_639_1': 'en', 'name': 'English'}]",Released,,Toy Story,False,7.7,5415`
	fields := parseLine(&line)

	t.Run("TestParseToyStoryMovie", func(t *testing.T) {
		expected := []*protocol.DataRow{
			{
				Data: &protocol.DataRow_Movie{
					Movie: &protocol.Movie{
						Id:            "862",
						ProdCountries: []string{"United States of America"},
						Title:         "Toy Story",
						Revenue:       373554033,
						Budget:        30000000,
						Overview:      "Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody from their owner, the duo eventually learns to put aside their differences.",
						ReleaseDate:   "1995-10-30",
						Genres:        []string{"Animation", "Comedy", "Family"},
					},
				},
			},
		}

		actual := parseMovie(fields)

		if len(actual) != len(expected) {
			t.Errorf("Expected %d items, but got %d", len(expected), len(actual))
		}

		for i := range actual {
			compareMovie(t, actual[i], expected[i])
		}
	})

	t.Run("TestParseMovieWithEmptyFields", func(t *testing.T) {
		actual := parseMovie([]string{})

		if actual != nil {
			t.Errorf("Expected nil, but got %v", actual)
		}
	})

	t.Run("TestParseMovieWithWrongFieldsLength", func(t *testing.T) {
		actual := parseMovie(fields[:10])

		if actual != nil {
			t.Errorf("Expected nil, but got %v", actual)
		}
	})

	t.Run("TestParseMovieWithNilFields", func(t *testing.T) {
		actual := parseMovie(nil)

		if actual != nil {
			t.Errorf("Expected nil, but got %v", actual)
		}
	})

	t.Run("TestParseMovieWithWrongFormatFieldsNonNumericRevenueAndBudgetShouldBeZero", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)
		expected := []*protocol.DataRow{
			{
				Data: &protocol.DataRow_Movie{
					Movie: &protocol.Movie{
						Id:            "862",
						ProdCountries: []string{"United States of America"},
						Title:         "Toy Story",
						Revenue:       0,
						Budget:        0,
						Overview:      "Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody from their owner, the duo eventually learns to put aside their differences.",
						ReleaseDate:   "1995-10-30",
						Genres:        []string{"Animation", "Comedy", "Family"},
					},
				},
			},
		}

		fields2[15] = "nonNumeric" //revenue
		fields2[2] = "nonNumeric"  //budget

		actual := parseMovie(fields2)

		if len(actual) != len(expected) {
			t.Errorf("Expected %d items, but got %d", len(expected), len(actual))
		}

		for i := range actual {
			compareMovie(t, actual[i], expected[i])
		}
	})
}

func compareRating(t *testing.T, actual, expected *protocol.DataRow) {
	if actual == nil || expected == nil {
		t.Errorf("Expected non-nil values, but got actual: %v, expected: %v", actual, expected)
		return
	}

	if actual.Data.(*protocol.DataRow_Rating).Rating.MovieId != expected.Data.(*protocol.DataRow_Rating).Rating.MovieId {
		t.Errorf("Expected MovieId %s, but got %s",
			expected.Data.(*protocol.DataRow_Rating).Rating.MovieId,
			actual.Data.(*protocol.DataRow_Rating).Rating.MovieId,
		)
	}

	if actual.Data.(*protocol.DataRow_Rating).Rating.Rating != expected.Data.(*protocol.DataRow_Rating).Rating.Rating {
		t.Errorf("Expected Rating %f, but got %f",
			expected.Data.(*protocol.DataRow_Rating).Rating.Rating,
			actual.Data.(*protocol.DataRow_Rating).Rating.Rating,
		)
	}
}

func TestParseRating(t *testing.T) {
	line := `1,110,1.0,1425941529`
	fields := parseLine(&line)

	t.Run("TestParseRating", func(t *testing.T) {
		expected := []*protocol.DataRow{
			{
				Data: &protocol.DataRow_Rating{
					Rating: &protocol.Rating{
						MovieId: "110",
						Rating:  1.0,
					},
				},
			},
		}

		actual := parseRating(fields)

		if len(actual) != len(expected) {
			t.Errorf("Expected %d items, but got %d", len(expected), len(actual))
		}

		for i := range actual {
			compareRating(t, actual[i], expected[i])
		}
	})

	t.Run("TestParseRatingWithEmptyFields", func(t *testing.T) {
		actual := parseRating([]string{})

		if actual != nil {
			t.Errorf("Expected nil, but got %v", actual)
		}
	})

	t.Run("TestParseRatingWithWrongFieldsLength", func(t *testing.T) {
		actual := parseRating(fields[:1])

		if actual != nil {
			t.Errorf("Expected nil, but got %v", actual)
		}
	})

	t.Run("TestParseRatingWithNilFields", func(t *testing.T) {
		actual := parseRating(nil)

		if actual != nil {
			t.Errorf("Expected nil, but got %v", actual)
		}
	})

	t.Run("TestParseRatingWithWrongFormatFieldsNonNumericRatingShouldBeZero", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)
		expected := []*protocol.DataRow{
			{
				Data: &protocol.DataRow_Rating{
					Rating: &protocol.Rating{
						MovieId: "110",
						Rating:  0.0,
					},
				},
			},
		}

		fields2[2] = "nonNumeric" //rating

		actual := parseRating(fields2)

		if len(actual) != len(expected) {
			t.Errorf("Expected %d items, but got %d", len(expected), len(actual))
		}

		for i := range actual {
			compareRating(t, actual[i], expected[i])
		}
	})
}
