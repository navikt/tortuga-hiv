
.PHONY: all build hiv hoi testapi

all: build test hiv hoi testapi

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

hiv:
	docker build -t navikt/tortuga-hiv --build-arg JAR_FILE=hiv-$(shell /bin/cat ./VERSION).jar hiv

hoi:
	docker build -t navikt/tortuga-hoi --build-arg JAR_FILE=hoi-$(shell /bin/cat ./VERSION).jar hoi

testapi:
	docker build -t navikt/tortuga-testapi --build-arg JAR_FILE=testapi-$(shell /bin/cat ./VERSION).jar testapi

push:
	docker push navikt/tortuga-hiv
	docker push navikt/tortuga-hoi
	docker push navikt/tortuga-testapi