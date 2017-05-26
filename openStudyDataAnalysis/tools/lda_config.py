# -*- coding: utf-8 -*-
pathConf = dict(
corpus_filename = '/home/pipi/files/DATASETS/SparkMLlib/sample_lda_data.txt',
SPARK_HOME = '/home/pipi/ENV/spark',
PYSPARK_PYTHON = '/home/pipi/ENV/ubuntu_env/bin/python',
SPARK_LOCAL_IP = '127.0.0.1',
JAVA_HOME = '/home/pipi/ENV/jdk')

ldaConf = dict(
K = 3,
alpha = 5,
beta = 5,
max_iter = 20 ,
seed = 0  ,
checkin_point_interval = 10  ,
optimizer = 'em')