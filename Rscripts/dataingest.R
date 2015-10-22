myData1 <- read.table("/home/student/adclickdata/feb_9/part-00000",sep=",",col.names=c("id","iplong", "agent", "pid","cid", "cntr", "time","category","referer"))
myData2 <- read.table("/home/student/adclickdata/feb_9/part-00001",sep=",",col.names=c("id","iplong", "agent", "pid","cid", "cntr", "time","category","referer"))
myData <- rbind(myData1,myData2)