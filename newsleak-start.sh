#!/bin/bash
cd /opt/newsleak
rm -f RUNNING_PID
cp -n -r conf /etc/settings/
cp -n -r data /etc/settings/
cp -n -r doc2vec-training /etc/settings/
chmod 777 -R /etc/settings

export NEWSLEAK_CONFIG=/etc/settings/conf/application.production.conf
#bin/newsleak -Dconfig.file=/etc/settings/conf/application.production.conf
