.PHONY: all download benchmark plot

all: download benchmark plot

download:
	./download.sh

benchmark:
	./benchmark.sh -b 40
	./benchmark.sh -b 100
	./benchmark.sh -b 1000
	./benchmark.sh

plot:
	uv run plot.py
