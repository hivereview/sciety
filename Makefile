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

.PHONY: backstop* build clean* dev find-* get* git-lfs ingest* install lint* prod release reports stop test* update*

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

unused-sass: node_modules find-unused-sass-declarations
	rm -f .purgecss/{full,purged}.css
	npx sass --no-source-map src/sass/style.scss:.purgecss/full.css
	npx purgecss --config purgecss.config.js --css .purgecss/full.css --output .purgecss/purged.css
	diff .purgecss/full.css .purgecss/purged.css

find-unused-sass-declarations: node_modules
	npx sass-unused 'src/**/*.scss'

lint: export TARGET = dev
lint: build unused-sass
	${DOCKER_COMPOSE} run --rm app npm run lint

lint\:fix: export TARGET = dev
lint\:fix: build unused-sass
	${DOCKER_COMPOSE} run --rm -e ESLINT=--fix -e STYLELINT=--fix app npm run lint

test: export TARGET = dev
test: build
	${DOCKER_COMPOSE} run --rm app npm run test

test\:coverage: export TARGET = dev
test\:coverage: build
	${DOCKER_COMPOSE} run --rm app npm run test:coverage
	sed -i -e 's/\/app\/src/src/g' coverage/coverage-final.json

backstop-test: export TARGET = dev
backstop-test: export DISABLE_COOKIEBOT = true
backstop-test: node_modules clean-db build
	${DOCKER_COMPOSE} up -d
	scripts/wait-for-healthy.sh
	${DOCKER_COMPOSE} exec -T db psql -c "copy events from '/data/backstop.csv' with CSV" sciety user
	${DOCKER_COMPOSE} restart app
	scripts/wait-for-healthy.sh
	npx backstop --docker test
	${DOCKER_COMPOSE} down

backstop-approve: export LATEST_TEST_PNG_FOLDER=$(shell ls -1 backstop_data/bitmaps_test/ | sort | tail -n 1)
backstop-approve: node_modules
	cp backstop_data/bitmaps_test/$$LATEST_TEST_PNG_FOLDER/backstop_default*.png backstop_data/bitmaps_reference/
	git status

build:
	$(DOCKER_COMPOSE) build app

install: node_modules git-lfs

node_modules: export PUPPETEER_SKIP_CHROMIUM_DOWNLOAD = true
node_modules: package.json package-lock.json
	npm install
	touch node_modules

git-lfs:
	git lfs install

clean:
	rm -rf .eslint .jest .stylelint build node_modules static/style.css static/style.css.map

clean-db: stop

stop:
	$(DOCKER_COMPOSE) down

ingest-events: export TARGET = dev
ingest-events: build
	$(DOCKER_COMPOSE) run -e INGEST_DEBUG=${INGEST_DEBUG} -e INGEST_ONLY=${INGEST_ONLY} app \
	npx ts-node src/ingest/update-event-data
	find data/reviews -name "*.csv" | xargs -n 1 ./scripts/review-events-to-jsonl.sh

update-event-data: ingest-events backstop-test

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

update-db-dump:
	kubectl run psql \
	--image=postgres:12.3 \
	--env=PGHOST=$$(kubectl get secret hive-prod-rds-postgres -o json | jq -r '.data."postgresql-host"'| base64 -d) \
	--env=PGDATABASE=$$(kubectl get secret hive-prod-rds-postgres -o json | jq -r '.data."postgresql-database"'| base64 -d) \
	--env=PGUSER=$$(kubectl get secret hive-prod-rds-postgres -o json | jq -r '.data."postgresql-username"'| base64 -d) \
	--env=PGPASSWORD=$$(kubectl get secret hive-prod-rds-postgres -o json | jq -r '.data."postgresql-password"'| base64 -d) \
	-- sleep 60
	kubectl wait --for condition=Ready pod psql
	kubectl exec psql -- psql -c "copy (select json_agg(events) from events) To STDOUT;" | sed -e 's/\\n//g' > ./events.json
	kubectl delete --wait=false pod psql
	gcloud config set project sciety
	gsutil cp events.json gs://sciety-data/events/events.json
	gsutil acl set public-read gs://sciety-data/events/events.json

taiko: export TARGET = dev
taiko: export AUTHENTICATION_STRATEGY = local
taiko: node_modules clean-db
	${DOCKER_COMPOSE} up -d
	scripts/wait-for-healthy.sh
	npx jest ${TEST} --testTimeout=300000 --bail --cache-directory=.jest-taiko --roots ./feature-test/
	${DOCKER_COMPOSE} down

regression: taiko

render-sanitised-markdown: node_modules
	npx ts-node --transpile-only ./scripts/hypothesis-review-render-testbed.ts

download-exploratory-test-from-prod:
	kubectl run psql \
	--image=postgres:12.3 \
	--env=PGHOST=$$(kubectl get secret hive-prod-rds-postgres -o json | jq -r '.data."postgresql-host"'| base64 -d) \
	--env=PGDATABASE=$$(kubectl get secret hive-prod-rds-postgres -o json | jq -r '.data."postgresql-database"'| base64 -d) \
	--env=PGUSER=$$(kubectl get secret hive-prod-rds-postgres -o json | jq -r '.data."postgresql-username"'| base64 -d) \
	--env=PGPASSWORD=$$(kubectl get secret hive-prod-rds-postgres -o json | jq -r '.data."postgresql-password"'| base64 -d) \
	-- sleep 60
	kubectl wait --for condition=Ready pod psql
	kubectl exec psql -- psql -c "COPY (SELECT * FROM events ORDER BY date ASC) TO STDOUT CSV;" > ./data/exploratory-test-from-prod.csv
	kubectl delete --wait=false pod psql

exploratory-test-from-prod: node_modules clean-db build
	${DOCKER_COMPOSE} up -d
	scripts/wait-for-healthy.sh
	${DOCKER_COMPOSE} exec -T db psql -c "COPY events FROM '/data/exploratory-test-from-prod.csv' WITH CSV" sciety user
	${DOCKER_COMPOSE} restart app
	scripts/wait-for-healthy.sh
	${DOCKER_COMPOSE} logs -f app

exploratory-test: node_modules clean-db build
	${DOCKER_COMPOSE} up -d
	scripts/wait-for-healthy.sh
	${DOCKER_COMPOSE} exec -T db psql -c "copy events from '/data/exploratory-test.csv' with CSV" sciety user
	${DOCKER_COMPOSE} restart app
	scripts/wait-for-healthy.sh
	${DOCKER_COMPOSE} logs -f app

download-db-dump-staging:
	kubectl run psql \
	--image=postgres:12.3 \
	--env=PGHOST=$$(kubectl get secret hive-staging-rds-postgres -o json | jq -r '.data."postgresql-host"'| base64 -d) \
	--env=PGDATABASE=$$(kubectl get secret hive-staging-rds-postgres -o json | jq -r '.data."postgresql-database"'| base64 -d) \
	--env=PGUSER=$$(kubectl get secret hive-staging-rds-postgres -o json | jq -r '.data."postgresql-username"'| base64 -d) \
	--env=PGPASSWORD=$$(kubectl get secret hive-staging-rds-postgres -o json | jq -r '.data."postgresql-password"'| base64 -d | sed -e 's/\$$\$$/$$$$$$$$/g') \
	-- sleep 600
	kubectl wait --for condition=Ready pod psql
	kubectl exec psql -- psql -c "copy (select json_agg(events) from events) To STDOUT;" | sed -e 's/\\n//g' > ./events-staging.json
	kubectl delete --wait=false pod psql

report-visits:
	@npx ts-node reports/visits.ts

get-ingress-logs:
	@export $$(cat .env | grep LOKI | xargs) && \
	logcli query -q -o raw --limit 600000 --batch 5000 \
	--timezone=UTC \
	--from="2021-09-27T00:00:00Z" \
	--to="2021-10-04T00:00:00Z" \
	'{app_kubernetes_io_name="ingress-nginx"}' > reports/ingress-logs.jsonl

get-error-logs:
	@export $$(cat .env | grep LOKI | xargs) && \
	logcli query -q -o raw --limit 600000 --batch 5000 \
	--timezone=UTC \
	--from="2021-09-10T00:00:00Z" \
	'{app_kubernetes_io_instance="sciety--prod"} | json | __error__="" | level = "error"'

