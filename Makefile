#
# Makefile for the img-db project.
#
# Targets:
#   all:        Clean out old build artifacts, build the project, and install it.
#   clean:      Remove all build artifacts.
#   build:      Build the project to the dist/ directory.
#   upload:     Upload the project to the test PyPI server.
#   test:       Create a virtual environment, install the project, and run the tests.
#
# Variables:
PROJECT_NAME := img-db
VENV_NAME := venv
PYTHON := python3
PACKAGE_NAME := img-db
TEST_PATH := tests

## Makefile targets

# Default target
all: clean build upload

clean:
	rm -rf dist $(VENV_NAME) $(TEST_PATH)/$(VENV_NAME)

build: $(VENV_NAME)
	$(VENV_NAME)/bin/python -m build

upload: build
	$(VENV_NAME)/bin/twine upload --repository testpypi dist/*

$(VENV_NAME):
	$(PYTHON) -m venv $(VENV_NAME)
	$(VENV_NAME)/bin/pip install --upgrade pip build twine
	# if requirements.txt exists, install it
	# test -f requirements.txt && $(VENV_NAME)/bin/pip install -r requirements.txt

test: $(VENV_NAME)
	cd $(TEST_PATH) && \
	$(PYTHON) -m venv $(VENV_NAME) && \
	$(VENV_NAME)/bin/pip install --upgrade pip && \
	$(VENV_NAME)/bin/pip install --index-url https://test.pypi.org/simple/ --no-deps $(PACKAGE_NAME) && \
	$(VENV_NAME)/bin/pip install pytest && \
	$(VENV_NAME)/bin/pytest --verbose
	rm -rf $(TEST_PATH)/$(VENV_NAME)


