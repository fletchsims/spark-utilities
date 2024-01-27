\#!/bin/sh
read -p "Subproject Name: " directoryName

if [[ ! -d "$directoryName" ]]; then
    echo "Directory $directoryName DOES NOT exists. Create it from build.sbt"
    exit
fi
scalaPath=${directoryName}/src/main/scala
scalaTestPath=${directoryName}/src/test/scala

# create folders
mkdir -p ${directoryName}/src/main/resources/{dev,prod,staging}
mkdir -p ${scalaPath}/com/arrow
mkdir -p ${scalaTestPath}/com/arrow

# create files
touch -a ${directoryName}/src/main/resources/{dev,prod,staging}/vars.conf
touch -a ${directoryName}/README.md

echo "Subfolders in $directoryName created successfully"

#0. This is for multi-project SBT builds only!!
#1. run build.sbt to create new module for your project
#2. open Terminal
#3. Two options:
#    a. run 'bash ./scripts/create_subfolders.sh' OR 'zsh ./scripts/create_subfolders.sh', OR
#    b. make script executable `chmod +x scripts/create_subfolders.sh` then use whenever without bash or zsh prefix