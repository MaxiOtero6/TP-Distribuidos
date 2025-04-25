cd ./test

ls ./actual_results.json &> /dev/null
if [ $? -ne 0 ]; then
    echo "Actual results not found..."
    echo "Run docker-compose up to generate actual results"
    exit 1
fi

ls ./expected_results.json &> /dev/null

if [ $? -ne 0 ]; then
    echo "Expected results not found..."
    echo "Running generator to create expected results"
    bash ../generate_expected.sh
fi

echo "Running comparison"
python3 compare_results.py

cd ..