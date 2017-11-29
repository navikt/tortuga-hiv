
.PHONY: all build docker hiv hoi testapi push

all: build test docker

build:
	docker run --rm -it \
		-v ${PWD}:/usr/src \
		-w /usr/src \
		-v ${HOME}/.m2:/root/.m2 \
		maven:3.5-jdk-8 mvn clean package -DskipTests=true -B -V

test:
	docker run --rm -it \
		-v ${PWD}:/usr/src \
		-w /usr/src \
		-v ${HOME}/.m2:/root/.m2 \
		maven:3.5-jdk-8 mvn test -B

docker: hiv hoi testapi

hiv:
	docker build -t navikt/tortuga-hiv -t navikt/tortuga-hiv:$(shell /bin/cat ./VERSION) --build-arg JAR_FILE=hiv-$(shell /bin/cat ./VERSION).jar hiv

hoi:
	docker build -t navikt/tortuga-hoi -t navikt/tortuga-hoi:$(shell /bin/cat ./VERSION) --build-arg JAR_FILE=hoi-$(shell /bin/cat ./VERSION).jar hoi

testapi:
	docker build -t navikt/tortuga-testapi -t navikt/tortuga-testapi:$(shell /bin/cat ./VERSION) --build-arg JAR_FILE=testapi-$(shell /bin/cat ./VERSION).jar testapi

push:
	docker push navikt/tortuga-hiv:latest
	docker push navikt/tortuga-hiv:$(shell /bin/cat ./VERSION)
	docker push navikt/tortuga-hoi:latest
	docker push navikt/tortuga-hoi:$(shell /bin/cat ./VERSION)
	docker push navikt/tortuga-testapi:latest
	docker push navikt/tortuga-testapi:$(shell /bin/cat ./VERSION)