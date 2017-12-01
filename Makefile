DOCKER  := docker
NAIS    := nais
VERSION := $(shell cat ./VERSION)

.PHONY: all build test docker hiv hoi testapi docker-push bump-version release

all: build test docker
release: tag docker-push

build:
	$(DOCKER) run --rm -it \
		-v ${PWD}:/usr/src \
		-w /usr/src \
		-v ${HOME}/.m2:/root/.m2 \
		maven:3.5-jdk-8 mvn clean package -DskipTests=true -B -V

test:
	$(DOCKER) run --rm -it \
		-v ${PWD}:/usr/src \
		-w /usr/src \
		-v ${HOME}/.m2:/root/.m2 \
		maven:3.5-jdk-8 mvn test -B

docker: hiv hoi testapi

hiv:
	$(NAIS) validate -f hiv/nais.yaml
	$(DOCKER) build -t navikt/tortuga-hiv -t navikt/tortuga-hiv:$(VERSION)  hiv

hoi:
	$(NAIS) validate -f hoi/nais.yaml
	$(DOCKER) build -t navikt/tortuga-hoi -t navikt/tortuga-hoi:$(VERSION) hoi

testapi:
	$(NAIS) validate -f testapi/nais.yaml
	$(DOCKER) build -t navikt/tortuga-testapi -t navikt/tortuga-testapi:$(VERSION) testapi

docker-push:
	$(DOCKER) push navikt/tortuga-hiv:latest
	$(DOCKER) push navikt/tortuga-hiv:$(VERSION)
	$(DOCKER) push navikt/tortuga-hoi:latest
	$(DOCKER) push navikt/tortuga-hoi:$(VERSION)
	$(DOCKER) push navikt/tortuga-testapi:latest
	$(DOCKER) push navikt/tortuga-testapi:$(VERSION)

bump-version:
	@echo $$(($$(cat ./VERSION) + 1)) > ./VERSION

tag:
	git add VERSION
	git commit -m "Bump version to $(VERSION) [skip ci]"
	git tag -a $(VERSION) -m "auto-tag from Travis CI [skip ci]"
