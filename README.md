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
sudo service hadoop-yarn-resourcemanager start
sudo service hadoop-yarn-nodemanager start 
sudo service hadoop-mapreduce-historyserver start
sudo su hdfs
hdfs dfs -mkdir /user/ankittharwani 
hdfs dfs -chown ankittharwani /user/ankittharwani
hdfs dfs -mkdir /user/Will 
hdfs dfs -chown Will /user/Will
sudo yum install hbase
sudo vi /etc/security/limits.conf
# hdfs  -       nofile  32768
# hdfs  -       nproc   2048
# hbase -       nofile  32768
# hbase -       nproc   2048
sudo vi /etc/hadoop/conf/hdfs-site.xml
# <property>
#   <name>dfs.datanode.max.transfer.threads</name>
#   <value>4096</value>
# </property>
for x in `cd /etc/init.d ; ls hadoop-hdfs-*` ; do sudo service $x restart ; done
sudo vi /etc/hbase/conf/hbase-site.xml
# <property>
#   <name>hbase.cluster.distributed</name>
#   <value>true</value>
# </property>
# <property>
#   <name>hbase.rootdir</name>
#   <value>hdfs://localhost:8020/hbase</value>
# </property>
sudo yum install zookeeper-server
sudo mkdir -p /var/lib/zookeeper
sudo chown -R zookeeper /var/lib/zookeeper/
sudo service zookeeper-server init
sudo service zookeeper-server start
sudo yum install hbase-master
sudo yum install hbase-rest
sudo service hbase-master start
sudo yum install hbase-regionserver
sudo service hbase-regionserver start
sudo yum install flume-ng
sudo yum install flume-ng-agent
sudo yum install hive
sudo yum install hive-metastore
sudo yum install hive-server2
sudo yum install hive-hbase
sudo yum install mysql-server
sudo service mysqld start
sudo yum install mysql-connector-java
sudo ln -s /usr/share/java/mysql-connector-java.jar /usr/lib/hive/lib/mysql-connector-java.jar
sudo /usr/bin/mysql_secure_installation
sudo /sbin/chkconfig mysqld on
sudo /sbin/chkconfig --list mysqld
# http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/cdh_ig_hive_metastore_configure.html
# create metastore database; create hive user on localhost; grant privileges; run /usr/lib/hive/bin/schematool -dbType mysql -initSchemaTo 1.1.0; configure hive-site.xml with the required mysql properties as given in the page
```

## Bugs and Issues
Email: ankittharwani@gmail.com, willie9monge@gmail.com

## Creator
Annawilkit

## Copyright and License
Slafka Inc
