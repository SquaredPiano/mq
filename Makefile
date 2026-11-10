.PHONY: test bench chart clean

PYTHON ?= python3

test:
	go vet ./... && go test -count=1 ./...

bench/broker.bench bench/replication.bench:
	mkdir -p bench
	go test ./internal/broker -run '^$$' -bench 'Benchmark(PublishInMemory|PublishDurable|PublishParallelDurable$$|PublishBatchDurable|FetchBatch32$$)' -benchtime=500ms -count=1 > bench/broker.bench
	go test ./internal/replication -run '^$$' -bench 'Benchmark(SynchronousPublish$$|SynchronousPublishParallel$$|SynchronousPublishBatch32|SynchronousPublishParallelWindow)' -benchtime=500ms -count=1 > bench/replication.bench

bench: bench/broker.bench bench/replication.bench

chart: bench/broker.bench bench/replication.bench
	@if [ -x bench/.venv/bin/python ]; then \
		bench/.venv/bin/python bench/render_chart.py bench/broker.bench bench/replication.bench -o docs/performance.png; \
	else \
		echo "bench/.venv missing; falling back to $(PYTHON)"; \
		$(PYTHON) bench/render_chart.py bench/broker.bench bench/replication.bench -o docs/performance.png; \
	fi

venv:
	python3 -m venv bench/.venv
	bench/.venv/bin/pip install -r bench/requirements.txt

clean:
	rm -f bench/broker.bench bench/replication.bench bench/values.json

sec:
	gosec -quiet -exclude=G115,G304 ./...
