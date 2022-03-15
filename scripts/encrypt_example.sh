#!/bin/bash
export AWS_REGION="`cat ./secrets/AWS_REGION.dev.secret`"
export AWS_ACCESS_KEY_ID="`cat ./secrets/AWS_ACCESS_KEY_ID.dev.secret`"
export AWS_SECRET_ACCESS_KEY="`cat ./secrets/AWS_SECRET_ACCESS_KEY.dev.secret`"
export ENCRYPTION_KEY="`cat ./secrets/ENCRYPTION_KEY.dev.secret`"
export ENCRYPTION_BYTES="`cat ./secrets/ENCRYPTION_BYTES.dev.secret`"
export ESURL="`cat ./secrets/ES_URL.prod.secret`"
export STREAM=''
export GROUPSIO_NO_INCREMENTAL=1
# export GROUPSIO_ST=1
# export GROUPSIO_NCPUS=1
# curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/last-update-cache/_delete_by_query" -d'{"query":{"term":{"key.keyword":"Groups.io:hyperledger+fabric"}}}' | jq -rS '.' || exit 1
../insights-datasource-github/encrypt "`cat ./secrets/email.secret`" > ./secrets/email.encrypted.secret || exit 2
../insights-datasource-github/encrypt "`cat ./secrets/password.secret`" > ./secrets/password.encrypted.secret || exit 3
./groupsio --groupsio-group-name='hyperledger+aries' --groupsio-save-archives=1 --groupsio-debug=0 --groupsio-es-url="${ESURL}" --groupsio-email="`cat ./secrets/email.encrypted.secret`" --groupsio-password="`cat ./secrets/password.encrypted.secret`" --groupsio-stream="${STREAM}" $* | tee run.log
