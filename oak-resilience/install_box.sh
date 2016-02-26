#!/bin/bash

vagrant box update --box ubuntu/trusty64

cd vagrant
vagrant up
vagrant halt
vagrant package
vagrant destroy --force
vagrant box add --name oak-resilience package.box
rm package.box
