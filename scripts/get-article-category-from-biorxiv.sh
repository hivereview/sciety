#!/bin/bash

category="$(curl -s https://api.biorxiv.org/details/biorxiv/$1 | jq -r .collection[0].category)"
echo $1,$category
