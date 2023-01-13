# PySpark

1. To use Pyspark locally, we can install the docker image from: 
https://hub.docker.com/r/jupyter/pyspark-notebook

2. Create a folder for exemple : c:/opt/mySparkCourse

3. To create a container and attach the folder, run:

```shell
docker run -p 8888:8888 --name mySparkCourse -v c:/opt/mySparkCourse/:/home/jovyan/work jupyter/pyspark-notebook
```

4. After running the container, go to log, and open the url : http://127.0.0.1:8888/?token=xxxxxx
