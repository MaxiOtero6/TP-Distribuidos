cd /test/
ls .venv

if [ $? -ne 0 ]; then
    echo "Virtual environment not found. Creating a new one..."
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
fi

echo "Running generator"
python3 generate_expected.py

cd ..