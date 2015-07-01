#options(java.parameters = "-Xmx5012m")
options(warn = -1)

## Memory management in rmr2 setting
backend.parameters = 
  list(
    hadoop = 
      list(
        D = "mapred.map.child.ulimit = 5242880",
        D = "mapred.reduce.child.ulimit = 5242880",
        D = "mapred.tasktracker.map.tasks.maximum = 2",
        D = "mapred.tasktracker.reduce.tasks.maximum = 2"))

### rhadoop and java setting
#Sys.setenv(HADOOP_HOME="/home/hadoop")
Sys.setenv(HADOOP_CMD="/home/hadoop/bin/hadoop") #Sys.getenv("HADOOP_CMD")
#Sys.setenv(HADOOP_STREAMING="/home/hadoop/contrib/streaming/hadoop-streaming.jar")
Sys.setenv(HADOOP_STREAMING="/home/hadoop/.versions/2.4.0-amzn-4/share/hadoop/tools/lib/hadoop-streaming-2.4.0-amzn-4.jar")
Sys.setenv(JAVA_HOME="/usr/java/latest") #Sys.setenv(JAVA_HOME="/usr/java/jdk1.7.0_71/")


### loading library
library(rhdfs, warn.conflicts = FALSE)
library(data.table, warn.conflicts = FALSE)
library(rmr2, warn.conflicts = FALSE)
library(plyrmr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)

### Initializes hdfs and setting options: 1.hadoop 2.local
hdfs.init()
rmr.options(backend = "hadoop") #, backend.parameters = backend.parameters)
plyrmr.options(backend = "hadoop")#, backend.parameters = backend.parameters)

### input and oupt data
args <- commandArgs(TRUE)
if(length(args) == 0){
  print("No arguments supplied.")
  q()
}else{
  hdfs.data <- file.path(args[[1]]) # hdfs.data <- file.path("/data/wifi_connection_log/input/2015-06-11")
  hdfs.out <- file.path(args[[2]]) # hdfs.out <- file.path("/user/hadoop/test_output")
}
#hdfs.data <- file.path("/user/hadoop/test_input/2015-06-29/")
#hdfs.data <- file.path("/user/hadoop/test_input/2015-06-29/ls.s3.ip-10-2-0-171.us-west-2.compute.internal.2015-06-29T00.58.part170.txt")
#hdfs.out <- file.path("hdfs:///user/hadoop/test_output/2015-06-30/")

### wifi connection log map function 
map = function(., v){
  #rmr.str(v) 
  wifi_data <- v
  colnames(wifi_data) <- c("tstamp", "ssid", "bssid", "smode", "channel", "mcc", "tstamp_tz", "network", "success", "lng",
                           "lat", "precision", "speed", "id", "uuid", "password", "action")
  ## filtering wifi speed condition
  L1 = 5600 # minimum : 56 KB/S
  L2 = 1000000 # maximum : 1 GB/S
  wifi_data_speed <- wifi_data %>% filter(speed > L1 & speed < L2)
  wifi_data_speed$ckey  <- sapply(wifi_data_speed$tstamp, function(x) unlist(str_split(as.character(x), pattern = " "))[3])
  wifi_data_speed$tstamp <- sapply(wifi_data_speed$tstamp, function(y) str_sub(y, start = 1, end = 10))
  return(keyval(wifi_data_speed$ckey, wifi_data_speed))#wifi_data_speed))
  rm(wifi_data_speed)
  gc()
}

### wifi connection log combine reduce function 
reduce = function(k, vv){
  CI_data <- mutate(vv, count =  dim(vv)[1], avg_speed =  mean(speed), 
                    cv_speed = ifelse(length(speed) >1, sd(speed), 0))  #transmute
  CI_data <- CI_data %>% select(tstamp, ssid, bssid, smode, mcc, count, avg_speed, cv_speed)
  #CI_data <- with(vv, count =  dim(vv)[1])
  #CI_data <- with(vv, avg_speed =  mean(speed))
  #CI_data <- with(vv, cv_speed = ifelse(length(speed) >1, sd(speed)/mean(speed), 0)) 
  return(keyval(k, CI_data[1, ]))
  rm(CI_data)
  rm(vv)
  gc()
}

### wifi connection log combine mapreduce function 
wifi_combine_data_mr <- function(input, output){
  mapreduce(
    input = input, 
    input.format = make.input.format("csv", sep = "\t"),
    map = map,
    reduce = reduce,
    output = output,
    output.format = make.output.format("csv", sep = "\t"),
    combine = FALSE,
    verbose = TRUE)
}

### run and output
#hdfs.rm(hdfs.out)
mr_output <- wifi_combine_data_mr(hdfs.data, hdfs.out) 