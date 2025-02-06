all::

# prevent attempt to download centralised config
PIPELINE_CONFIG_FILES=.dummy
init::; touch .dummy

include makerules/makerules.mk
include makerules/pipeline.mk
include makerules/datapackage.mk
include makerules/development.mk

DB=dataset/digital-land.sqlite3
DB_PERF = dataset/performance.sqlite3

ifeq ($(PARQUET_DIR),)
PARQUET_DIR=data/
endif
ifeq ($(PARQUET_SPECIFICATION_DIR),)
export PARQUET_SPECIFICATION_DIR=$(PARQUET_DIR)specification/
endif
ifeq ($(PARQUET_PERFORMANCE_DIR),)
export PARQUET_PERFORMANCE_DIR=$(PARQUET_DIR)performance/
endif


DATASTORE_URL = https://files.planning.data.gov.uk/

first-pass::
	mkdir -p dataset/
	bin/download-collection.sh
	bin/download-pipeline.sh
	bin/concat.sh
	bin/download-issues.sh
	bin/download-operational-issues.sh
	bin/download-column-field.sh
	bin/download-converted-resources.sh
	#bin/download-resources.sh
	python3 bin/concat-issues.py
	python3 bin/concat-column-field.py
	python3 bin/concat-converted-resource.py
	python3 bin/download_expectations.py


second-pass:: $(DB) 

third-pass:: $(DB_PERF)

$(DB):	bin/load.py
	@rm -f $@
	mkdir -p $(PARQUET_SPECIFICATION_DIR)
	python3 bin/load.py $@

$(DB_PERF): bin/load_reporting_tables.py bin/load_performance.py
	bin/download-digital-land.sh
	@rm -f $@  
	mkdir -p $(PARQUET_PERFORMANCE_DIR)
	python3 bin/load_reporting_tables.py $@ $(DB)
	python3 bin/load_performance.py $@ $(DB)

clean::
	rm -rf ./var

clobber::
	rm -rf var/collection
	rm -rf var/pipeline
	rm -rf dataset/
	rm -rf $(DB)
	rm -rf $(DB_PERF)
	rm -rf $(PARQUET_SPECIFICATION_DIR)
	rm -rf $(PARQUET_PERFORMANCE_DIR)
	rm -rf $(PARQUET_DIR)

clobber-performance::
	rm -rf $(DB_PERF)

aws-build::
	aws batch submit-job --job-name digital-land-db-$(shell date '+%Y-%m-%d-%H-%M-%S') --job-queue dl-batch-queue --job-definition dl-batch-def --container-overrides '{"environment": [{"name":"BATCH_FILE_URL","value":"https://raw.githubusercontent.com/digital-land/docker-builds/main/builder_run.sh"}, {"name" : "REPOSITORY","value" : "digital-land-builder"}]}'

push::
	aws s3 cp $(DB) s3://digital-land-collection/digital-land.sqlite3
	aws s3 cp $(DB_PERF) s3://digital-land-collection/performance.sqlite3
	
specification::
	# additional
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/issue-type.csv' > specification/issue-type.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/severity.csv' > specification/severity.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/cohort.csv' > specification/cohort.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/project.csv' > specification/project.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/project-organisation.csv' > specification/project-organisation.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/include-exclude.csv' > specification/include-exclude.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/role.csv' > specification/role.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/role-organisation.csv' > specification/role-organisation.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/role-organisation-rule.csv' > specification/role-organisation-rule.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/specification.csv' > specification/specification.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/specification-status.csv' > specification/specification-status.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/project-status.csv' > specification/project-status.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/provision.csv' > specification/provision.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/provision-rule.csv' > specification/provision-rule.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/provision-reason.csv' > specification/provision-reason.csv
