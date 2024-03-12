# installReqs
echo "Installing dependencies..."
pip install ".[dev,tests]"
pip install ".[main]"
echo "Done installing dependencies."

# flakeCheck
echo "Running flake check..."
pflake8 --config pyproject.toml ./
echo "Done running flake check."

# test
# install test reqs
# pip install ".[tests]"
# We already did this but putting everything in order for due diligence right now

# run tests
echo "Running unit tests and coverage checks..."
coverage run --data-file=unit_tests/.coverage.testPython --rcfile=pyproject.toml -m pytest -s unit_tests -c pytest.ini
echo "Done running unit tests."


