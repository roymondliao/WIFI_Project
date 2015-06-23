#options(java.parameters = "-Xmx5012m")
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
Sys.setenv(HADOOP_CMD="/home/hadoop/bin/hadoop")
#Sys.setenv(HADOOP_STREAMING="/home/hadoop/contrib/streaming/hadoop-streaming.jar")
Sys.setenv(HADOOP_STREAMING="/home/hadoop/.versions/2.4.0-amzn-4/share/hadoop/tools/lib/hadoop-streaming-2.4.0-amzn-4.jar")
Sys.setenv(JAVA_HOME="/usr/java/latest") #Sys.setenv(JAVA_HOME="/usr/java/jdk1.7.0_71/")
Sys.getenv("HADOOP_CMD")
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
  hdfs.data <- file.path(args[[1]]) 
  hdfs.out <- file.path(args[[2]])
}

### wifi connection log combine map function 
map = function(., v){
  wifi_data = v
  #rmr.str(v) 
  colnames(wifi_data) <- c("tstamp", "ssid", "bssid", "smode", "channel", "mcc", "tstamp_tz", "network", "success", "lng",
                           "lat", "precision", "speed", "id", "uuid", "password", "action")
  wifi_data_speed <- wifi_data %>% filter(wifi_data$speed != 0)
  wifi_data_speed$ckey  <- sapply(wifi_data_speed$tstamp, function(x) unlist(str_split(as.character(x), pattern = " "))[3])
  wifi_data_speed$tstamp <- sapply(wifi_data_speed$tstamp, function(y) str_sub(y, start = 1, end = 10))
  return(keyval(wifi_data_speed$ckey, wifi_data_speed))
}

### wifi connection log combine reduce function 
reduce = function(k, vv){
  ## filtering the wifi speed by confidence interval with each countries(mcc)
  L1 = 0 # minimum : 0 kb
  L2 = 1000000 # maximum : 1,000,000 kb
  val_data = vv
  rmr.str(vv) 
  if(nrow(val_data) > 1){
    CI_data <- val_data %>% filter(L2 > speed & speed > L1) %>% select(ssid, bssid, smode, mcc, ckey, speed)
  }else{
    CI_data <- val_data %>% select(ssid, bssid, smode, mcc, ckey, speed)
  }
  ## create sample / standard division / average table
  CI_data <- mutate(CI_data, count =  dim(CI_data)[1])
  CI_data <- mutate(CI_data, avg_speed =  mean(speed))
  CI_data <- mutate(CI_data, sd_speed = ifelse(length(speed) >1, sd(speed), 0)
  #rm(CI_data)
  gc()
  return(keyval(CI_data$ckey, CI_data))
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
    combine =TRUE,
    verbose = TRUE)
}

### run and output
mr_output <- wifi_combine_data_mr(hdfs.data, hdfs.out) 
