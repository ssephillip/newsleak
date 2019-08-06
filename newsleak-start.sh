#!/bin/bash
cd /opt/newsleak
rm -f RUNNING_PID
who
cp -n -r conf /etc/settings/
cp -n -r data /etc/settings/
cp -n -r doc2vec-training /etc/settings/
chmod -R 777 /etc/settings/
export NEWSLEAK_CONFIG=/etc/settings/conf/application.production.conf
bin/newsleak -Dconfig.file=/etc/settings/conf/application.production.conf
