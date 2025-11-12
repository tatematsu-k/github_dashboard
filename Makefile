.PHONY: install collect generate clean help

help:
	@echo "Available commands:"
	@echo "  make install    - Install Python dependencies"
	@echo "  make collect    - Collect GitHub data"
	@echo "  make generate   - Generate HTML report"
	@echo "  make clean      - Clean generated files"
	@echo "  make all        - Run collect and generate"

install:
	python3 -m pip install --upgrade pip
	python3 -m pip install -r requirements.txt

collect:
	@if [ -z "$$GITHUB_TOKEN" ]; then \
		echo "Error: GITHUB_TOKEN environment variable is not set"; \
		exit 1; \
	fi
	python3 scripts/collect_data.py

generate:
	python3 scripts/generate_html.py

all: collect generate

clean:
	rm -rf data/collected_data.json docs/index.html __pycache__ scripts/__pycache__
