package utils

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
)

// mapJsonRegex maps a JSON string using a regex pattern and returns the first group of each match.
func mapJsonRegex(json string, regex string) []string {
	re := regexp.MustCompile(regex)
	matches := re.FindAllStringSubmatch(json, -1)

	var mappedMatches []string
	for _, match := range matches {
		if len(match) > 1 {
			mappedMatches = append(mappedMatches, match[1])
		}
	}

	return mappedMatches
}

// mapJsonRegexTuple maps a JSON string using a regex pattern and returns the first group of each match as a tuple.
// It's the multiple item version of mapJsonRegex.
func mapJsonRegexTuple(json string, regex string, items int) [][]string {
	re := regexp.MustCompile(regex)
	matches := re.FindAllStringSubmatch(json, -1)

	var mappedMatches [][]string
	for _, match := range matches {
		if len(match) > items {
			mappedMatches = append(mappedMatches, match[1:])
		}
	}

	return mappedMatches
}

// parseLine parses a CSV line and returns the fields as a slice of strings.
// It handles quoted fields and commas inside quotes.
// It also handles empty fields.
// It returns a slice of strings containing the fields.
func parseLine(line *string) (fields []string) {
	var currentField strings.Builder
	inField := false

	for _, char := range *line {
		if char == ',' && !inField {
			fields = append(fields, currentField.String())
			currentField.Reset()
			continue
		}

		if char == '"' {
			inField = !inField
			continue
		}

		currentField.WriteRune(char)
	}

	// Append last field
	fields = append(fields, currentField.String())

	return fields
}

// parseMovie parses a movie line and returns a slice of DataRow with one item.
func parseMovie(fields []string) []*protocol.DataRow {
	// adult,belongs_to_collection,budget,genres,homepage,
	// id,imdb_id,original_language,original_title,overview,
	// popularity,poster_path,production_companies,production_countries,release_date,
	// revenue,runtime,spoken_languages,status,tagline,
	// title,video,vote_average,vote_count
	rawProdCountries := fields[13]
	rawGenres := fields[3]

	regex := `'name': '([^']+)'`
	prodCountries := mapJsonRegex(rawProdCountries, regex)
	genres := mapJsonRegex(rawGenres, regex)

	rawRevenue := fields[16]
	rawBudget := fields[2]

	revenue, err := strconv.ParseUint(rawRevenue, 10, 64)

	if err != nil {
		revenue = 0
	}

	budget, err := strconv.ParseUint(rawBudget, 10, 64)

	if err != nil {
		revenue = 0
	}

	id := fields[5]
	title := fields[21]
	overview := fields[9]
	releaseDate := fields[15]

	return []*protocol.DataRow{
		{
			Data: &protocol.DataRow_Movie{
				Movie: &protocol.Movie{
					Id:            id,
					ProdCountries: prodCountries,
					Title:         title,
					Revenue:       revenue,
					Budget:        budget,
					Overview:      overview,
					ReleaseDate:   releaseDate,
					Genres:        genres,
				},
			},
		},
	}
}

// parseRating parses a rating line and returns a slice of DataRow with one item.
func parseRating(fields []string) []*protocol.DataRow {
	//userId,movieId,rating,timestamp
	rawRating := fields[2]

	rating, err := strconv.ParseFloat(rawRating, 32)

	if err != nil {
		rating = 0.0
	}

	return []*protocol.DataRow{
		{
			Data: &protocol.DataRow_Rating{
				Rating: &protocol.Rating{
					MovieId: fields[1],
					Rating:  float32(rating),
				},
			},
		},
	}
}

// parseCredit parses a credit line and returns a slice of DataRow with many items as actors in the movie.
// It returns a slice of DataRow with one item for each actor.
func parseCredit(fields []string) []*protocol.DataRow {
	// cast,crew,id
	rawCast := fields[0]
	regex := `'id': (\d+).*?'name': '([^']+)'`
	cast := mapJsonRegexTuple(rawCast, regex, 2)

	var ret []*protocol.DataRow

	for _, actor := range cast {
		id := actor[0]
		name := actor[1]

		ret = append(ret,
			&protocol.DataRow{
				Data: &protocol.DataRow_Credit{
					Credit: &protocol.Credit{
						MovieId: fields[2],
						ActorId: id,
						Name:    name,
					},
				},
			},
		)
	}

	return ret
}

func ParseRow(row *string, fileType protocol.FileType) ([]*protocol.DataRow, error) {
	fields := parseLine(row)

	switch fileType {
	case protocol.FileType_MOVIES:
		return parseMovie(fields), nil
	case protocol.FileType_CREDITS:
		return parseCredit(fields), nil
	case protocol.FileType_RATINGS:
		return parseRating(fields), nil
	case protocol.FileType_EOF: //TODO
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown file type: %s", fileType)
	}
}
