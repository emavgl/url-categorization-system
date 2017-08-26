# Installation Notes

## Prerequisites
- Hadoop 2.7.3
- Spark 2.2.0
- PySpark
- Python3
- JDK8
- Scala

Define $HADOOP_HOME and $SPARK_HOME variables
configure your $PATH variables by adding the following lines
in your ~/.bashrc (or ~/.zshrc) file:

- `export HADOOP_HOME=/home/hado/hadoop`
- `export SPARK_HOME=/home/hado/spark`
- `export PATH=$SPARK_HOME/bin:$PATH`
- `export PATH=$PATH:$HADOOP_HOME/bin`

Now, if the command *pyspark* should start a new python shell
with additional spark context *sc*.

If you want to work with Jupyter Notebook it is suggested to use this script bash:

```
$!/usr/bin/bash
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
exec pyspark
```

Save it in a file pyspark-jupyter and put it into `/usr/local/bin/pyspark-jupyter`
now, place in the directory you want, and run the command `pyspark-jupyter`
Now the jupyter notebook should open in your browser.

To check whether Spark is correctly linked create a new Python file inside Jupyter Notebook, type sc and run that line. You should see something different than an error.

Otherwise you could use findspark and pyspark package from pip, see: https://blog.sicara.com/get-started-pyspark-jupyter-guide-tutorial-ae2fe84f594f

### Debug
Simply use pyspark (cli or jupyter notebook) to test your application; do not care about
performances, simply try if it works or raise an error.

### Deploy
Ok, it works. Now, we are interested in performances. To do this and exploit spark, we
have to run the application on a cluster, this means that we need a driver (which launch the command to the worker) and workers (that executes the tasks).

#### Standalone and Client Mode

If you are running an interactive shell, e.g. pyspark (CLI or via an IPython notebook), by default you are running in **client mode**: client-mode means that the driver runs on a dedicated server (Master node) inside a dedicated process. For other information visit this link https://stackoverflow.com/questions/37027732/spark-standalone-differences-between-client-and-cluster-deploy-modes

From the online documentation
Alternatively, if your application is submitted from a machine far from the worker machines (e.g. locally on your laptop), it is common to use cluster mode to minimize network latency between the drivers and the executors. **Currently, standalone mode does not support cluster mode for Python applications.**

*standalone mode*: cluster manager provided by spark, if you want to use cluster mode for Python
you should try with hadoop yarn or others (https://spark.apache.org/docs/latest/submitting-applications.html)

Since we are interested in *debug and see the performances* of our machine, we can test it using the *standlone mode* and *client mode* with workers in the same machine or located in different machine in the same network, in that way we minimze network latency between the drivers and the executors.

#### Drivers

`$SPARK_HOME/sbin/start-master.sh`

Visit the page: http://localhost:8080/ which contains information of the cluster
Just take the URL and put it on the file *spark-defaults.conf* eg.

`spark.master                     spark://emanuele-K53SV:7077`

If you run the an interactive shell you will se a new program under the Running Execution header.





