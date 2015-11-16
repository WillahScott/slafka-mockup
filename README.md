# [Slafka Mockup]
Template code and mockup for Slafka Dashboard

Demo:
http://wrapbootstrap.com/preview/WB06R48S4

## Getting Started
For web dashboard - refer to docs for more details. We'd be using the angular version of the app.

For setting up instance - 
```
yum search httpd
sudo yum install httpd.x86_64
sudo yum install git
sudo groupadd slafkadev
sudo usermod -a -G slafkadev ankittharwani
sudo usermod -a -G slafkadev Will
sudo mkdir /apps
sudo mkdir /apps/slafka
sudo chgrp slafkadev /apps/slafka/
sudo chown ankittharwani /apps/slafka/
cd /apps/slafka/
git clone git@gitlab.com:w205-slafka/slafka-mockup.git
sudo mkdir /var/www/html/slafka
sudo cp -r angular/* /var/www/html/slafka/
sudo service httpd start
cd /apps/slafka/
mkdir softwares
cd softwares/
wget http://archive.cloudera.com/cdh5/one-click-install/redhat/6/x86_64/cloudera-cdh-5-0.x86_64.rpm
sudo yum --nogpgcheck localinstall cloudera-cdh-5-0.x86_64.rpm
sudo rpm --import http://archive.cloudera.com/cdh5/redhat/6/x86_64/cdh/RPM-GPG-KEY-cloudera
sudo yum install hadoop-conf-pseudo
sudo yum install java-1.8.0-openjdk.x86_64
sudo su hdfs
hdfs namenode -format
exit
for x in `cd /etc/init.d ; ls hadoop-hdfs-*` ; do sudo service $x start ; done
sudo /usr/lib/hadoop/libexec/init-hdfs.sh
```

## Bugs and Issues
Email: ankittharwani@gmail.com

## Creator
Annawilkit

## Copyright and License
Slafka Inc
