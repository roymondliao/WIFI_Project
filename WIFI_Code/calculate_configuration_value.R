# setting configuration value and calculation value (mb)
configuration.table <- data.frame()
containers = 6
RAM.per.container =  1920

configuration.table["yarn.nodemanager.resource.memory.mb", 1] = containers * RAM.per.container #4096
configuration.table["yarn.scheduler.minimum.allocation.mb", 1] = RAM.per.container #2048
configuration.table["yarn.scheduler.maximum.allocation.mb", 1] = containers * RAM.per.container #4096
configuration.table["mapreduce.map.memory.mb", 1] = RAM.per.container #2048
configuration.table["mapreduce.reduce.memory.mb", 1] = 2 * RAM.per.container #4096
configuration.table["mapreduce.map.java.opts", 1] = 0.8 * RAM.per.container #1638
configuration.table["mapreduce.reduce.java.opts", 1] = 0.8 * 2 * RAM.per.container #3277
configuration.table["yarn.app.mapreduce.am.resource.mb", 1] = 2 * RAM.per.container #4096
configuration.table["yarn.app.mapreduce.am.command.opts", 1] = 0.8 * 2 * RAM.per.container #3277
configuration.table
