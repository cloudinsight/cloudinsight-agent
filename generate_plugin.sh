#!/bin/bash

if [[ $1 ]]; then
    cd collector/plugins
    if [ ! -d $1 ]; then
        mkdir $1
        sed "s/example/$1/g;s/Example/${1^}/g" example/example.go > $1/$1.go
        sed "s/example/$1/g;s/Example/${1^}/g" example/example_test.go > $1/$1_test.go
    else
        echo "The plugin exists. Do nothing.";
        exit 1;
    fi
else
    echo "Usage: ./generate_plugin.sh plugin_name";
    exit 1;
fi
