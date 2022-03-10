#!/bin/bash

# update every currently installed package 
sudo yum update

# install git
sudo yum install git

# install python3
sudo yum install python3

# Install MySQL
# Step 1: Install the MySQL Yum Repository

sudo yum localinstall /net/homesrv/repo/pkgs/mysql/mysql80-community-release-el7-1.noarch.rpm

# Step 2: Enable the MySQL version

sudo yum-config-manager --enable mysql57-community
sudo yum-config-manager --disable mysql80-community

# Step 3: Use yum to install/update mysql packages

sudo yum install mysql-community-server
