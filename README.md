# Project
Architecture Cloud Hybrid - Integrating Hadoop(AWS) with Big Query(GCP) 

# Environment

Installation Ambari and HDP 2.6:

sudo yum -y install openssh-server wget ntp ntpdate
sudo ntpdate -q 0.rhel.pool.ntp.org
sudo systemctl enable ntpd.service
sudo systemctl start ntpd.service
sudo systemctl stop firewalld.service	
sudo systemctl disable firewalld.service

sudo sed -i 's/SELINUX=enforcing/SELINUX=disabled/g' /etc/sysconfig/selinux

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys
chmod 700 ~/.ssh
chmod 600 ~/.ssh/*

cd /etc/yum.repos.d/

sudo wget http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.6.0.0/ambari.repo
sudo wget http://public-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.6.3.0/hdp.repo

sudo yum clean all
sudo yum clean dbcache
sudo yum clean metadata
sudo yum makecache
sudo rpm -rebuilddb
sudo yum history new
sudo yum repolist

#IF RED HAT7 AWS
#sudo yum-config-manager --enable rhui-REGION-rhel-server-optional

sudo yum install -y gcc yum-priorities yum-plugin-priorities createrepo yum-plugin-priorities yum-priorities yum-utils unzip psmisc redhat-lsb nc ambari-agent
sudo systemctl enable ambari-agent
sudo systemctl start ambari-agent
sudo systemctl status ambari-agent

sudo yum install -y ambari-server
sudo ambari-server setup
sudo systemctl enable ambari-server.service
sudo systemctl start ambari-server.service
sudo systemctl status ambari-server.service

sudo python get-pip.py

sudo pip install pandas hive hdfs pyhive Thrift sasl thrift_sasl

sudo yum install -y gcc-c++ python-devel.x86_64 cyrus-sasl-devel.x86_64 cyrus-sasl-gssapi-* cyrus-sasl-ldap-* cyrus-sasl-ntlm-* cyrus-sasl-* cyrus-sasl-plain-* cyrus-sasl-lib-* cyrus-sasl-md5-* cyrus-sasl-devel-* cyrus-sasl-sql-*

# Estruct logic deploy and hdfs by area

HDFS :: /<area>/projeto/subprojeto
  ex: http://hdp01:50070/atendimento/call_center/email/...
  
Deploy Local :: <path_deploy>/name_project + version/subdirectories
  ex: /path_deploy/cadastro_email-0.0.1/java
                                 .../hql
                                 .../python
                                 .../sh
  
# Deploy
Configure some variables on configuration_functions.py and configure_deploy.py, setter values of environment
Execute /deploy_path/python/configure_deploy.py

