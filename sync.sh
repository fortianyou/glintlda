#encoding=utf-8
#########################################################################
# File Name: sync.sh
# Author: GuoTianyou
# mail: fortianyou@gmail.com
# Created Time: Thu Mar 23 16:04:13 2017
#! /bin/bash

#########################################################################
JAR=classes/artifacts/glintlda_ps_sparse_jar/
#zip -d $JAR 'META-INF/*.SF' 'META-INF/*.RSA' 'META-INF/*SF'
scp -r $JAR guotianyou@bda07:/home/gty/LDA/
