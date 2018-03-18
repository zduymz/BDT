# Project 23

## Description
This is a part of course CS523 (MUM).

Purpose:
- Using jupyter notebook for demo
- Using SparkSQL and Hive to process and store data
- Using Plot.ly to show a graph

## Setup
- Platform: Virtual Box, image can be dowloaded from [here](https://www.cloudera.com/downloads/quickstart_vms/5-13.html)

- Install packages:
```shell
sudo pip install jupyter jupyter_contrib_nbextensions pyspark jupyter_spark plotly
jupyter contrib nbextension install --user
jupyter nbextension install --py jupyter_spark
jupyter serverextension enable --py jupyter_spark
nbextension enable --py widgetsnbextension
```

- Run jupyter notebook:
```shell
jupyter notebook --ip=0.0.0.0

```

- Open notebook Project23.ipynb and experience yourself.