#!/usr/bin/env bash

set -euo pipefail

cd ~
mkdir mysql
cd mysql

wget http://dev.mysql.com/get/mysql57-community-release-el7-9.noarch.rpm
yum -y --nogpgcheck install mysql57-community-release-el7-9.noarch.rpm

yum -y install mysql-community-devel
ln -s /usr/lib64/mysql/libmysqlclient.a /usr/lib64/libmysqlclient.a

yum clean all
rm -rf ~/mysql
