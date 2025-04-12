package utils

import (
	"testing"
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
}


