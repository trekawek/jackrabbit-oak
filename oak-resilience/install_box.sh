#!/bin/bash

BASE=ubuntu/wily64
vagrant box add "$BASE" || vagrant box update --box "$BASE"

cd vagrant
vagrant up
vagrant halt
vagrant package
vagrant destroy --force
vagrant box add --name oak-resilience --force package.box
rm package.box
