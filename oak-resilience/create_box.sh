#!/bin/bash

cd vagrant
vagrant up
vagrant halt
vagrant package
vagrant destroy --force
