#!/bin/bash

cd vagrant
rm -f package.box
vagrant up
vagrant halt
vagrant package
vagrant destroy --force
vagrant box add --name oak-resilience package.box
rm package.box
