.PHONY: all run server test bench bundle verify clean

all: test

run:
	python zenith.py repl

server:
	python zenith.py server

test:
	python tests/run_all.py

bench:
	python zenith.py bench --ops 10000 --concurrency 8

bundle:
	python tools/bundle.py

verify:
	python zenith.py verify-deps

clean:
	python -c "import shutil, os, glob; [shutil.rmtree(p, ignore_errors=True) for p in ['data', 'data_bench'] + glob.glob('**/__pycache__', recursive=True)]"
