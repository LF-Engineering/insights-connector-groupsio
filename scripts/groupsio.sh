#!/bin/bash
# ESENV=prod|test
if [ -z "${ESENV}" ]
then
  ESENV=test
fi
./groupsio --groupsio-url='https://wiki.lfnetworking.org' --groupsio-es-url="`cat ./secrets/ES_URL.${ESENV}.secret`" --groupsio-email="`cat ./secrets/email.secret`" --groupsio-password="`cat ./secrets/password.secret`" $*
