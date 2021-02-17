ifeq (${TARGET},)
TARGET := dev
endif

DOCKER_COMPOSE = docker-compose --file docker-compose.yml --file docker-compose.$(TARGET).yml
DATA_VOLUME := $(shell pwd)
IMAGE := sciety/sciety
IMAGE_TAG := local
PORT := 8080

export IMAGE
export IMAGE_TAG

.PHONY: backstop build clean* dev find-* install lint* prod release test* update-event-data

dev: export TARGET = dev
dev: .env install build
	${DOCKER_COMPOSE} up --abort-on-container-exit --exit-code-from app

prod: export TARGET = prod
prod: .env build
	${DOCKER_COMPOSE} up --abort-on-container-exit --exit-code-from app

.env:
	cp .env.example .env

.gcp-ncrc-key.json:
	gcloud iam service-accounts keys create ./.gcp-ncrc-key.json --iam-account ncrc-sheet@sciety.iam.gserviceaccount.com

lint: export TARGET = dev
lint: build
	${DOCKER_COMPOSE} run --rm app npm run lint

lint\:fix: export TARGET = dev
lint\:fix: build
	${DOCKER_COMPOSE} run --rm -e ESLINT=--fix -e STYLELINT=--fix app npm run lint

test: export TARGET = dev
test: build
	${DOCKER_COMPOSE} run --rm app npm run test

test\:coverage: export TARGET = dev
test\:coverage: build
	${DOCKER_COMPOSE} run --rm app npm run test:coverage

backstop: node_modules
	npx backstop --docker reference > /tmp/backstop_reference.log
	npx backstop --docker test > /tmp/backstop_test.log

build:
	$(DOCKER_COMPOSE) build app

install: node_modules

node_modules: export PUPPETEER_SKIP_CHROMIUM_DOWNLOAD = true
node_modules: package.json package-lock.json
	npm install
	touch node_modules

clean:
	rm -rf .eslint .jest build node_modules

clean-db:
	$(DOCKER_COMPOSE) down

find-review-commons-reviews: export TARGET = dev
find-review-commons-reviews: build
	$(DOCKER_COMPOSE) run -T app \
		npx ts-node scripts/find-reviews-from-hypothesis NEGQVabn > ./data/reviews/316db7d9-88cc-4c26-b386-f067e0f56334.csv

find-elife-reviews: export TARGET = dev
find-elife-reviews: build
	$(DOCKER_COMPOSE) run -T app \
		npx ts-node scripts/find-reviews-from-hypothesis q5X6RWJ6 > ./data/reviews/b560187e-f2fb-4ff9-a861-a204f3fc0fb0.csv

find-peerj-reviews: export TARGET = dev
find-peerj-reviews: build
	$(DOCKER_COMPOSE) run -T app \
		npx ts-node scripts/find-reviews-from-crossref-via-biorxiv 10.7717 10.7287 > ./data/reviews/53ed5364-a016-11ea-bb37-0242ac130002.csv

find-pci-reviews: export TARGET = dev
find-pci-reviews: build
	$(DOCKER_COMPOSE) run -T app \
		npx ts-node scripts/find-reviews-from-pci

find-prereview-reviews: export TARGET = dev
find-prereview-reviews: build
	$(DOCKER_COMPOSE) run -T app \
		npx ts-node scripts/find-reviews-from-prereview > ./data/reviews/10360d97-bf52-4aef-b2fa-2f60d319edd7.csv

COMMUNITY_SCRIPTS := \
	find-review-commons-reviews \
	find-elife-reviews \
	find-peerj-reviews \
	find-pci-reviews \
	find-prereview-reviews

sort-event-data:
	find data -type f | xargs -I % sort -g -o % %

update-event-data: $(COMMUNITY_SCRIPTS) sort-event-data

release: export TAG = latest/$(shell date +%Y%m%d%H%M)
release:
	git tag $$TAG
	git push origin $$TAG

prod-sql:
	kubectl run psql \
	--rm -it --image=postgres:12.3 \
	--env=PGHOST=$$(kubectl get secret hive-prod-rds-postgres -o json | jq -r '.data."postgresql-host"'| base64 -d) \
	--env=PGDATABASE=$$(kubectl get secret hive-prod-rds-postgres -o json | jq -r '.data."postgresql-database"'| base64 -d) \
	--env=PGUSER=$$(kubectl get secret hive-prod-rds-postgres -o json | jq -r '.data."postgresql-username"'| base64 -d) \
	--env=PGPASSWORD=$$(kubectl get secret hive-prod-rds-postgres -o json | jq -r '.data."postgresql-password"'| base64 -d) \
	-- psql

taiko: export TARGET = dev
taiko: clean-db
	${DOCKER_COMPOSE} up -d
	scripts/wait-for-healthy.sh
	npx jest ${TEST} --testTimeout=300000 --bail --roots ./feature-test/
	${DOCKER_COMPOSE} down

regression: taiko backstop

render-sanitised-markdown: node_modules
	npx ts-node --transpile-only ./scripts/hypothesis-review-render-testbed.ts
