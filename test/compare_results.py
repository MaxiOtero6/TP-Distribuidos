import json
from typing import Any

QUERY1 = "Query1"
QUERY2 = "Query2"
QUERY3 = "Query3"
QUERY4 = "Query4"
QUERY5 = "Query5"

RESET = '\033[0m'
BRIGHT_RED = '\033[91m'
BRIGHT_GREEN = '\033[92m'
BRIGHT_CYAN = '\033[96m'


def load_json(file_path: str):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


def compare_query1(expected: dict[str, Any], actual: dict[str, Any]):
    expected_query = expected[QUERY1]
    actual_query = actual[QUERY1]

    if len(expected_query) != len(actual_query):
        print(
            f"Query1: Expected {BRIGHT_GREEN}{len(expected_query)}{RESET} results, but got {BRIGHT_RED}{len(actual_query)}{RESET}.")

    expected_titles = {item["title"] for item in expected_query}
    actual_titles = {item["title"] for item in actual_query}
    if expected_titles != actual_titles:
        # set to list and sort
        expected_titles_err = [
            i for i in expected_titles if i not in actual_titles]
        actual_titles_err = [
            i for i in actual_titles if i not in expected_titles]
        print(
            f"Query1: Expected titles {BRIGHT_GREEN}{expected_titles_err}{RESET}, but got {BRIGHT_RED}{actual_titles_err}{RESET}.")
    for expected_item in expected_query:
        for actual_item in actual_query:
            if expected_item["title"] == actual_item["title"]:
                if expected_item["genres"] != actual_item["genres"]:
                    print(
                        f"Query1: Expected {BRIGHT_CYAN}{expected_item['title']}{RESET} genres to be {BRIGHT_GREEN}{expected_item['genres']}{RESET}, but got {BRIGHT_RED}{actual_item['genres']}{RESET}.")
                break


def compare_query2(expected: dict[str, Any], actual: dict[str, Any]):
    expected_query = expected[QUERY2]
    actual_query = actual[QUERY2]

    if len(expected_query) != len(actual_query):
        print(
            f"Query2: Expected {BRIGHT_GREEN}{len(expected_query)} results, but got {len(actual_query)}.")

    for key in expected_query.keys():
        if key not in actual_query:
            print(
                f"Query2: Expected key {BRIGHT_CYAN}{key}{RESET} not found in actual results.")
        else:
            if expected_query[key] != actual_query[key]:
                print(
                    f"Query2: Expected {BRIGHT_CYAN}{key}{RESET} to be {BRIGHT_GREEN}{expected_query[key]}{RESET}, but got {BRIGHT_RED}{actual_query[key]}{RESET}; diff: {BRIGHT_CYAN}{abs(expected_query[key] - actual_query[key])}{RESET}.")


def compare_query3(expected: dict[str, Any], actual: dict[str, Any]):
    expected_query = expected[QUERY3]
    actual_query = actual[QUERY3]

    if len(expected_query) != len(actual_query):
        print(
            f"Query3: Expected {len(expected_query)} results, but got {len(actual_query)}.")

    for key in expected_query.keys():
        if key not in actual_query:
            print(
                f"Query3: Expected {BRIGHT_GREEN}{len(expected_query)} results, but got {len(actual_query)}.")
        else:
            expected_value = expected_query[key]
            actual_value = actual_query[key]

            if expected_value["title"] != actual_value["title"]:
                print(
                    f"Query3: Expected {BRIGHT_CYAN}{key}{RESET} title to be {BRIGHT_GREEN}{expected_value['title']}{RESET}, but got {BRIGHT_RED}{actual_value['title']}{RESET}.")
            if expected_value["rating"] != actual_value["rating"]:
                print(
                    f"Query3: Expected {BRIGHT_CYAN}{key}{RESET} rating to be {BRIGHT_GREEN}{expected_value['rating']}{RESET}, but got {BRIGHT_RED}{actual_value['rating']}{RESET}; diff: {BRIGHT_CYAN}{abs(expected_value['rating'] - actual_value['rating'])}{RESET}.")


def compare_query4(expected: dict[str, Any], actual: dict[str, Any]):
    expected_query = expected[QUERY4]
    actual_query = actual[QUERY4]

    if len(expected_query) != len(actual_query):
        print(
            f"Query4: Expected {BRIGHT_GREEN}{len(expected_query)} results, but got {len(actual_query)}.")

    expected_names = {item["name"] for item in expected_query}
    actual_names = {item["name"] for item in actual_query}
    if expected_names != actual_names:
        expected_names_err = [
            i for i in expected_names if i not in actual_names]
        actual_names_err = [
            i for i in actual_names if i not in expected_names]
        print(
            f"Query4: Expected names {BRIGHT_GREEN}{expected_names_err}{RESET}, but got {BRIGHT_RED}{actual_names_err}{RESET}.")
    for expected_item in expected_query:
        for actual_item in actual_query:
            if expected_item["name"] == actual_item["name"]:
                if expected_item["count"] != actual_item["count"]:
                    print(
                        f"Query4: Expected {BRIGHT_CYAN}{expected_item['name']}{RESET} count to be {BRIGHT_GREEN}{expected_item['count']}{RESET}, but got {BRIGHT_RED}{actual_item['count']}{RESET}.")
                break


def compare_query5(expected: dict[str, Any], actual: dict[str, Any]):
    expected_query = expected[QUERY5]
    actual_query = actual[QUERY5]

    if len(expected_query) != len(actual_query):
        print(
            f"Query5: Expected {BRIGHT_GREEN}{len(expected_query)} results, but got {len(actual_query)}.")

    for key in expected_query.keys():
        if key not in actual_query:
            print(
                f"Query5: Expected key {BRIGHT_CYAN}{key}{RESET} not found in actual results.")
        else:
            if expected_query[key] != actual_query[key]:
                print(
                    f"Query5: Expected {BRIGHT_CYAN}{key}{RESET} to be {BRIGHT_GREEN}{expected_query[key]}{RESET}, but got {BRIGHT_RED}{actual_query[key]}{RESET}; diff: {BRIGHT_CYAN}{abs(expected_query[key] - actual_query[key])}{RESET}.")


if __name__ == "__main__":
    expected = load_json('expected_results.json')
    actual = load_json('actual_results.json')

    compare_query1(expected, actual)
    compare_query2(expected, actual)
    compare_query3(expected, actual)
    compare_query4(expected, actual)
    compare_query5(expected, actual)
