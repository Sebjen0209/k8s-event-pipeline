CLUSTER      ?= event-pipeline
IMAGE_PREFIX ?= event-pipeline
TAG          ?= dev

.PHONY: test build docker-build kind-up kind-load deploy smoke e2e kind-down

test:
	go vet ./...
	go test -race -count=1 ./...

build:
	go build ./...

docker-build:
	docker build --build-arg SERVICE=ingest-api -t $(IMAGE_PREFIX)/ingest-api:$(TAG) .
	docker build --build-arg SERVICE=worker -t $(IMAGE_PREFIX)/worker:$(TAG) .

kind-up:
	kind create cluster --name $(CLUSTER)

kind-load:
	kind load docker-image $(IMAGE_PREFIX)/ingest-api:$(TAG) $(IMAGE_PREFIX)/worker:$(TAG) --name $(CLUSTER)

deploy:
	helm upgrade --install event-pipeline deploy/chart/event-pipeline \
		--set image.repository=$(IMAGE_PREFIX) \
		--set image.tag=$(TAG) \
		--set image.pullPolicy=Never \
		--wait --timeout 180s

smoke:
	./hack/smoke.sh http://localhost:18080

# Full local run: build, load, deploy, then drive traffic through the Service.
e2e: docker-build kind-load deploy
	kubectl port-forward svc/ingest-api 18080:80 & \
	PF=$$!; sleep 3; \
	./hack/smoke.sh http://localhost:18080; RC=$$?; \
	kill $$PF 2>/dev/null; exit $$RC

kind-down:
	kind delete cluster --name $(CLUSTER)
