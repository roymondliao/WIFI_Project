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
  
  return(keyval(wifi_data_speed$mcc, wifi_data_speed))
}

### wifi connection log combine reduce function 
reduce = function(k, vv){
  ## confidence interval function
  mean.range = function(x, alpha, sd) {
    n = length(x) # sample
    mx = mean(x) # avarage
    r1 = qnorm(alpha/2) # parameter low
    r2 = qnorm(1-alpha/2) # parameter top
    L1 = mx-r2*sd/sqrt(n) # confidence interval low
    L2 = mx-r1*sd/sqrt(n) # confidence interval top
    range = data.frame(L1 = ifelse(L1<0, 0, L1), avg = mx, L2 = L2)
    return(range)
  }
  
  ## filtering the wifi speed by confidence interval with each countries(mcc)
  CI_filter <- function(val_data){
    CI_table <- mean.range(x = val_data$speed, alpha = 0.01, sd = sd(val_data$speed))
    CI_data <- val_data %>% filter(CI_table$L2 > speed, speed > CI_table$L1)
    return(CI_data)
  }
  CI_filter_data <- CI_filter(vv) # return the final data 
  ## precision compare (more small more precision)
  Speed_table <- CI_filter_data %>% group_by(ckey) %>% summarise(connection_count = n(), m.speed = mean(speed))
  final_data <- inner_join(CI_filter_data, Speed_table)  
  final_data$speed <- final_data$m.speed
  Precision_table <- final_data %>% group_by(ckey) %>% summarise(prec.min = min(precision))
  final_data <- inner_join(final_data, Precision_table, by = "ckey")
  final_data$precision <- final_data$prec.min
  final_data <- final_data[, -which(colnames(final_data) == "m.speed")]
  final_data <- final_data[, -which(colnames(final_data) == "prec.min")]
  #final_wifi_data <- final_data %>% group_by(ckey) %>% distinct()  
  rm(final_dat, CI_filter_data, Speed_table, Precision_table)
  gc()
  return(keyval(final_data$ckey, final_data))
}

map2 <- function(., v){
  return(keyval(v$ckey, v))
}

reduce2 <- function(k2, v2){
  return(keyval(k2, v2[1, ]))
  #final_wifi_data %>% group_by(ckey) %>% summarise(count = n())  
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
#first_mr_data <- wifi_combine_data_mr(hdfs.data, hdfs.out) 
first_mr_data <- mapreduce(input = hdfs.data, 
                           input.format = make.input.format("csv", sep = "\t"), 
                           map = map, 
                           reduce = reduce,
                           combine = FALSE,
                           verbose = TRUE)

second_mr_data <- mapreduce(input = first_mr_data, 
                            output = hdfs.out, 
                            output.format = make.output.format("csv", sep = "\t"),
                            map = map2, reduce = reduce2)