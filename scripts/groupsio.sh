#!/bin/bash
# ESENV=prod|test
if [ -z "${ESENV}" ]
then
  ESENV=test
fi
./groupsio --groupsio-group-name='hyperledger+fabric' --groupsio-es-url="`cat ./secrets/ES_URL.${ESENV}.secret`" --groupsio-email="`cat ./secrets/email.secret`" --groupsio-password="`cat ./secrets/password.secret`" $*
