#!/bin/sh

~/spark/bin/spark-submit --master spark://ip-10-64-10-119:7077 --conf "spark.executor.memory=100g" --class cosc282.Homework /mnt/wangs/COSC282_Homework-assembly-0.0.1.jar --jars /mnt/wangs/cloud9-2.0.2-SNAPSHOT-fatjar.jar,/mnt/wangs/common-1.0.1.jar --files /mnt/wangs/application.conf
