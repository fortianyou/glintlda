#encoding=utf-8
#########################################################################
# File Name: sync.sh
# Author: GuoTianyou
# mail: fortianyou@gmail.com
# Created Time: Thu Mar 23 16:04:13 2017
#! /bin/bash

#########################################################################
JAR=classes/artifacts/glintlda_jar/glintlda.jar
#zip -d $JAR 'META-INF/*.SF' 'META-INF/*.RSA' 'META-INF/*SF'
scp $JAR root@bda07:/home/gty/LDA/
