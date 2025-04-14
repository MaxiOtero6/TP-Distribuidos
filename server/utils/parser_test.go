package utils

import (
	"testing"

	"github.com/MaxiOtero6/TP-Distribuidos/server/model"
)

const MOVIE_LINE = `False,"{'id': 10194, 'name': 'Toy Story Collection', 'poster_path': '/7G9915LfUQ2lVfwMEEhDsn3kT4B.jpg', 'backdrop_path': '/9FBwqcd9IRruEDUrTdcaafOMKUq.jpg'}",30000000,"[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]",http://toystory.disney.com/toy-story,862,tt0114709,en,Toy Story,"Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody from their owner, the duo eventually learns to put aside their differences.",21.946943,/rhIRbceoE9lR4veEXuwCC2wARtG.jpg,"[{'name': 'Pixar Animation Studios', 'id': 3}]","[{'iso_3166_1': 'US', 'name': 'United States of America'}]",1995-10-30,373554033,81.0,"[{'iso_639_1': 'en', 'name': 'English'}]",Released,,Toy Story,False,7.7,5415`
const RATING_LINE = `1,110,1.0,1425941529`
const CREDIT_LINE = `"[{'cast_id': 14, 'character': 'Woody (voice)', 'credit_id': '52fe4284c3a36847f8024f95', 'gender': 2, 'id': 31, 'name': 'Tom Hanks', 'order': 0, 'profile_path': '/pQFoyx7rp09CJTAb932F2g8Nlho.jpg'}, {'cast_id': 15, 'character': 'Buzz Lightyear (voice)', 'credit_id': '52fe4284c3a36847f8024f99', 'gender': 2, 'id': 12898, 'name': 'Tim Allen', 'order': 1, 'profile_path': '/uX2xVf6pMmPepxnvFWyBtjexzgY.jpg'}]","[{'credit_id': '52fe4284c3a36847f8024f49', 'department': 'Directing', 'gender': 2, 'id': 7879, 'job': 'Director', 'name': 'John Lasseter', 'profile_path': '/7EdqiNbr4FRjIhKHyPPdFfEEEFG.jpg'}, {'credit_id': '52fe4284c3a36847f8024f4f', 'department': 'Writing', 'gender': 2, 'id': 12891, 'job': 'Screenplay', 'name': 'Joss Whedon', 'profile_path': '/dTiVsuaTVTeGmvkhcyJvKp2A5kr.jpg'}]",862`

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
}

func compareMovie(t *testing.T, actual, expected *model.Movie) {
	if actual == nil || expected == nil {
		t.Errorf("Expected non-nil values, but got actual: %v, expected: %v", actual, expected)
		return
	}

	if actual.Id != expected.Id {
		t.Errorf("Expected Id %s, but got %s",
			expected.Id,
			actual.Id,
		)
	}

	if actual.Title != expected.Title {
		t.Errorf("Expected Title %s, but got %s",
			expected.Title,
			actual.Title,
		)
	}

	if !compareSlicesOrdered(actual.ProdCountries, expected.ProdCountries) {
		t.Errorf("Expected ProdCountries %s, but got %s",
			expected.ProdCountries[0],
			actual.ProdCountries[0],
		)
	}

	if actual.Revenue != expected.Revenue {
		t.Errorf("Expected Revenue %d, but got %d",
			expected.Revenue,
			actual.Revenue,
		)
	}

	if actual.Budget != expected.Budget {
		t.Errorf("Expected Budget %d, but got %d",
			expected.Budget,
			actual.Budget,
		)
	}

	if actual.Overview != expected.Overview {
		t.Errorf("Expected Overview %s, but got %s",
			expected.Overview,
			actual.Overview,
		)
	}

	if actual.ReleaseYear != expected.ReleaseYear {
		t.Errorf("Expected ReleaseYear %d, but got %d",
			expected.ReleaseYear,
			actual.ReleaseYear,
		)
	}

	if !compareSlicesOrdered(actual.Genres, expected.Genres) {
		t.Errorf("Expected %d Genres, but got %d",
			len(expected.Genres),
			len(actual.Genres),
		)
	}
}

func TestParseMovie(t *testing.T) {

	line := MOVIE_LINE
	fields := parseLine(&line)

	t.Run("TestParseToyStoryMovie", func(t *testing.T) {
		expected := []*model.Movie{
			{
				Id:            "862",
				ProdCountries: []string{"United States of America"},
				Title:         "Toy Story",
				Revenue:       373554033,
				Budget:        30000000,
				Overview:      "Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody from their owner, the duo eventually learns to put aside their differences.",
				ReleaseYear:   1995,
				Genres:        []string{"Animation", "Comedy", "Family"},
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

		if len(actual) != 0 {
			t.Errorf("Expected zero items, but got %v", len(actual))
		}
	})

	t.Run("TestParseMovieWithWrongFieldsLength", func(t *testing.T) {
		actual := parseMovie(fields[:10])

		if len(actual) != 0 {
			t.Errorf("Expected zero items, but got %v", len(actual))
		}
	})

	t.Run("TestParseMovieWithNilFields", func(t *testing.T) {
		actual := parseMovie(nil)

		if len(actual) != 0 {
			t.Errorf("Expected zero items, but got %v", len(actual))
		}
	})

	t.Run("TestParseMovieWithWrongFormatFieldsNonNumericRevenue", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[15] = "nonNumeric" //revenue

		actual := parseMovie(fields2)

		if len(actual) != 0 {
			t.Errorf("Expected zero items, but got %d", len(actual))
		}
	})

	t.Run("TestParseMovieWithWrongFormatFieldsNonNumericBudget", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[2] = "nonNumeric" //budget

		actual := parseMovie(fields2)

		if len(actual) != 0 {
			t.Errorf("Expected zero items, but got %d", len(actual))
		}
	})

	t.Run("TestParseMovieWithWrongFormatFieldsNonNumericReleaseYear", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[14] = "nonNumeric" //release date

		actual := parseMovie(fields2)

		if len(actual) != 0 {
			t.Errorf("Expected zero items, but got %d", len(actual))
		}
	})
}

func compareRating(t *testing.T, actual, expected *model.Rating) {
	if actual == nil || expected == nil {
		t.Errorf("Expected non-nil values, but got actual: %v, expected: %v", actual, expected)
		return
	}

	if actual.MovieId != expected.MovieId {
		t.Errorf("Expected MovieId %s, but got %s",
			expected.MovieId,
			actual.MovieId,
		)
	}

	if actual.Rating != expected.Rating {
		t.Errorf("Expected Rating %f, but got %f",
			expected.Rating,
			actual.Rating,
		)
	}
}

func TestParseRating(t *testing.T) {
	line := RATING_LINE
	fields := parseLine(&line)

	t.Run("TestParseRating", func(t *testing.T) {
		expected := []*model.Rating{
			{
				MovieId: "110",
				Rating:  1.0,
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

		if len(actual) != 0 {
			t.Errorf("Expected zero items, but got %v", actual)
		}
	})

	t.Run("TestParseRatingWithWrongFieldsLength", func(t *testing.T) {
		actual := parseRating(fields[:1])

		if len(actual) != 0 {
			t.Errorf("Expected zero items, but got %v", actual)
		}
	})

	t.Run("TestParseRatingWithNilFields", func(t *testing.T) {
		actual := parseRating(nil)

		if len(actual) != 0 {
			t.Errorf("Expected zero items, but got %v", actual)
		}
	})

	t.Run("TestParseRatingWithWrongFormatFieldsNonNumericRating", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[2] = "nonNumeric" //rating

		actual := parseRating(fields2)

		if len(actual) != 0 {
			t.Errorf("Expected zero items, but got %d", len(actual))
		}
	})
}

func compareCredit(t *testing.T, actual, expected *model.Actor) {
	if actual == nil || expected == nil {
		t.Errorf("Expected non-nil values, but got actual: %v, expected: %v", actual, expected)
		return
	}

	if actual.MovieId != expected.MovieId {
		t.Errorf("Expected MovieId %s, but got %s",
			expected.MovieId,
			actual.MovieId,
		)
	}

	if actual.Id != expected.Id {
		t.Errorf("Expected Id %s, but got %s",
			expected.Id,
			actual.Id,
		)
	}

	if actual.Name != expected.Name {
		t.Errorf("Expected Name %s, but got %s",
			expected.Name,
			actual.Name,
		)
	}
}

func TestParseCredits(t *testing.T) {
	line := CREDIT_LINE
	fields := parseLine(&line)

	t.Run("TestParseRating", func(t *testing.T) {
		var expected []*model.Actor

		expected = append(expected, &model.Actor{
			MovieId: "862",
			Id:      "31",
			Name:    "Tom Hanks",
		})

		expected = append(expected, &model.Actor{
			MovieId: "862",
			Id:      "12898",
			Name:    "Tim Allen",
		})

		actual := parseCredit(fields)

		if len(actual) != len(expected) {
			t.Errorf("Expected %d items, but got %d", len(expected), len(actual))
		}

		for i := range actual {
			compareCredit(t, actual[i], expected[i])
		}
	})

	t.Run("TestParseRatingWithEmptyFields", func(t *testing.T) {
		actual := parseCredit([]string{})

		if len(actual) != 0 {
			t.Errorf("Expected zero items, but got %v", len(actual))
		}
	})

	t.Run("TestParseRatingWithWrongFieldsLength", func(t *testing.T) {
		actual := parseCredit(fields[:1])

		if len(actual) != 0 {
			t.Errorf("Expected  zero items, but got %v", len(actual))
		}
	})

	t.Run("TestParseRatingWithNilFields", func(t *testing.T) {
		actual := parseCredit(nil)

		if len(actual) != 0 {
			t.Errorf("Expected  zero items, but got %v", len(actual))
		}
	})
}

// func TestParseRow(t *testing.T) {
// 	t.Run("TestParseRowWithMovie", func(t *testing.T) {
// 		line := MOVIE_LINE
// 		actual, err := ParseRow(&line, protocol.FileType_MOVIES)

// 		if err != nil {
// 			t.Errorf("Unexpected error: %v", err)
// 		}

// 		if len(actual) != 1 {
// 			t.Errorf("Expected 1 item, but got %d", len(actual))
// 		}

// 		if actual[0].Data.(*protocol.DataRow_Movie) == nil {
// 			t.Errorf("Expected DataRow_Movie, but got %T", actual[0].Data)
// 		}
// 	})

// 	t.Run("TestParseRowWithRating", func(t *testing.T) {
// 		line := RATING_LINE

// 		actual, err := ParseRow(&line, protocol.FileType_RATINGS)

// 		if err != nil {
// 			t.Errorf("Unexpected error: %v", err)
// 		}

// 		if len(actual) != 1 {
// 			t.Errorf("Expected 1 item, but got %d", len(actual))
// 		}

// 		if actual[0].Data.(*protocol.DataRow_Rating) == nil {
// 			t.Errorf("Expected DataRow_Movie, but got %T", actual[0].Data)
// 		}
// 	})

// 	t.Run("TestParseRowWithCredits", func(t *testing.T) {
// 		line := CREDIT_LINE

// 		actual, err := ParseRow(&line, protocol.FileType_CREDITS)

// 		if err != nil {
// 			t.Errorf("Unexpected error: %v", err)
// 		}

// 		if len(actual) != 2 {
// 			t.Errorf("Expected 2 item, but got %d", len(actual))
// 		}

// 		if actual[0].Data.(*protocol.DataRow_Credit) == nil {
// 			t.Errorf("Expected DataRow_Movie, but got %T", actual[0].Data)
// 		}

// 		if actual[1].Data.(*protocol.DataRow_Credit) == nil {
// 			t.Errorf("Expected DataRow_Movie, but got %T", actual[1].Data)
// 		}
// 	})

// 	t.Run("TestParseEOFLine", func(t *testing.T) {
// 		line := ""
// 		actual, err := ParseRow(&line, protocol.FileType_EOF)

// 		if err != nil {
// 			t.Errorf("Unexpected error: %v", err)
// 		}

// 		if actual != nil {
// 			t.Errorf("Expected nil, but got %v", actual)
// 		}
// 	})

// 	t.Run("TestParseRowWithInvalidFileType", func(t *testing.T) {
// 		line := MOVIE_LINE
// 		actual, err := ParseRow(&line, protocol.FileType(999))

// 		if err == nil {
// 			t.Errorf("Expected error, but got nil")
// 		}

// 		if actual != nil {
// 			t.Errorf("Expected nil, but got %v", actual)
// 		}
// 	})
// }
