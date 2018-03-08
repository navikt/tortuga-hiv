DOCKER  := docker
NAIS    := nais
VERSION := $(shell cat ./VERSION)
REGISTRY:= repo.adeo.no:5443

.PHONY: all build test docker hiv hoi testapi docker-push bump-version release manifest

all: build test docker
release: tag docker-push

build:
	$(DOCKER) run --rm -t \
		-v ${PWD}:/usr/src \
		-w /usr/src \
		-u $(shell id -u) \
		-v ${HOME}/.m2:/var/maven/.m2 \
		-e MAVEN_CONFIG=/var/maven/.m2 \
		maven:3.5-jdk-8 mvn -Duser.home=/var/maven clean package -DskipTests=true -B -V

test:
	$(DOCKER) run --rm -t \
		-v ${PWD}:/usr/src \
		-w /usr/src \
		-u $(shell id -u) \
		-v ${HOME}/.m2:/var/maven/.m2 \
		-e MAVEN_CONFIG=/var/maven/.m2 \
		maven:3.5-jdk-8 mvn -Duser.home=/var/maven test -B

docker: hiv hoi testapi

hiv:
	$(NAIS) validate -f hiv/nais.yaml
	$(DOCKER) build --pull -t $(REGISTRY)/tortuga-hiv -t $(REGISTRY)/tortuga-hiv:$(VERSION) hiv

hoi:
	$(NAIS) validate -f hoi/nais.yaml
	$(DOCKER) build --pull -t $(REGISTRY)/tortuga-hoi -t $(REGISTRY)/tortuga-hoi:$(VERSION) hoi

testapi:
	$(NAIS) validate -f testapi/nais.yaml
	$(DOCKER) build --pull -t $(REGISTRY)/tortuga-testapi -t $(REGISTRY)/tortuga-testapi:$(VERSION) testapi

docker-push:
	$(DOCKER) push $(REGISTRY)/tortuga-hiv:$(VERSION)
	$(DOCKER) push $(REGISTRY)/tortuga-hoi:$(VERSION)
	$(DOCKER) push $(REGISTRY)/tortuga-testapi:$(VERSION)

bump-version:
	@echo $$(($$(cat ./VERSION) + 1)) > ./VERSION

tag:
	git add VERSION
	git commit -m "Bump version to $(VERSION) [skip ci]"
	git tag -a $(VERSION) -m "auto-tag from Makefile"

manifest:
	nais upload --app tortuga-hiv -v $(VERSION) -f ./tortuga-hiv/nais.yaml
	nais upload --app tortuga-hoi -v $(VERSION) -f ./tortuga-hoi/nais.yaml
	nais upload --app tortuga-testapi -v $(VERSION) -f ./tortuga-testapi/nais.yaml
