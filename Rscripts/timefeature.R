library("sqldf")
data_stats <- function(n){
  x <- numeric(n)
  y <- numeric(n)
  for(i in 1:n){
    pub_time <- sqldf(sprintf("Select * from myData where pid='%s'",i))
    pub_time$time <- strptime(pub_time$time,"%Y-%m-%d %H:%M:%S")
    N = length(pub_time$time)
    breaks <- seq(as.POSIXct("2012-02-09 00:00:00"),as.POSIXct("2012-02-12 00:00:00"),by="10 sec")
    bar <- hist(pub_time$time,breaks,plot=FALSE)
    x[i]<-max(bar$counts, na.rm = TRUE)
    y[i]<-mean(bar$counts)
    
  }
  data.frame(x,y);
}
within(df1, { timestamp=format(as.POSIXct(paste(D, Time)), "%d/%m/%Y %H:%M:%S") })
myData$time <- strptime(myData$time,"%H:%M:%S")
myData <- read.table("/home/yash/Downloads/joined click fraud data/whole_dataset_with_date_time_joined_9_feb.csv",sep=",",col.names=c("id","iplong", "agent", "pid","cid", "cntr", "date","category","referer","status"))
myData$date <- format(as.POSIXct(paste(myData$date, myData$time), format="%Y-%m-%d %H:%M:%S"))
myData <- subset(myData,select = c(1,2,3,4,5,6,7,9,10,11))
newMyData <- subset(myData,select = c(1,2,3,4,5,6,8,9,10))
write.table(file="/home/yash/Downloads/joined click fraud data/whole_dataset_with_date_time_joined_9_feb.csv",x=myData,sep=",",row.names = FALSE,col.names = FALSE)

myData$date <- strptime(myData$date,"%Y-%m-%d %H:%M:%S")

breaks <- seq(as.POSIXct("2012-02-09 00:00:00"),as.POSIXct("2012-02-12 00:00:00"),by="60 sec")
pid <- unique(myData$pid)
data_stats <- function(){
  x <- numeric(3081)
  z <- numeric(3081)
  for(i in pid){
    pub_time <- myData[myData$pid == i,]
    bar <- hist(pub_time$date,breaks,plot=FALSE)
    x[i]<-max(bar$counts, na.rm = TRUE)
    z[i]<-i
  }
  a <- data.frame(z,x);
  write.table(file="/home/yash/Downloads/joined click fraud data/timefeature_60secs_feb_9",x=a,sep=",",row.names = FALSE,col.names = FALSE)
}
data_stats()

newTimeFeature <- read.table("/home/yash/Downloads/joined click fraud data/timefeature_60secs_feb_9",sep=",",col.names = c("pid","max"))
newTimeFeature <- subset(timefeature,select=c(1,2))
newTimeFeature[newTimeFeature==0]<-NA
na.omit(newTimeFeature)
partner_iplong<-sqldf("Select pid, count(DISTINCT iplong),count(Distinct agent), count(Distinct cid),count(Distinct cntr),category,count(Distinct referer),status from newMyData group by pid")
total <- merge(partner_iplong,newTimeFeature,by="pid")
total<-total[,c(1,2,3,4,5,6,7,9,8)]
write.table(file = "/home/yash/Downloads/joined click fraud data/counts_with_time_60_feb9",x=total,sep=",",row.names = FALSE,col.names = FALSE)
total<-read.table("/home/yash/Downloads/joined click fraud data/counts_with_time_60_feb9",sep=",",col.names = c("pid","iplong","agent","cid","cntr","category","referer","max","status"))
newtotal <- total[,c(2,3,4,5,6,7,8,9)]
write.table(file = "/home/yash/Downloads/joined click fraud data/counts_with_time_60_without_partnerid_feb9",x=newtotal,sep=",",row.names = FALSE,col.names = FALSE)

//23 feb data feature extraction

myData <- read.table("/home/yash/Downloads/joined click fraud data/whole_dataset_with_date_time_23_feb.csv",sep=",",col.names=c("id","iplong", "agent", "pid","cid", "cntr", "date","time","category","referer","status"))
myData$date <- format(as.POSIXct(paste(myData$date, myData$time), format="%Y-%m-%d %H:%M:%S"))
myData <- subset(myData,select = c(1,2,3,4,5,6,7,9,10,11))
myData$date <- strptime(myData$date,"%Y-%m-%d %H:%M:%S")
write.table(file="/home/yash/Downloads/joined click fraud data/whole_dataset_with_date_time_joined_23_feb.csv",x=myData,sep=",",row.names = FALSE,col.names = FALSE)
breaks <- seq(as.POSIXct("2012-02-23 00:00:00"),as.POSIXct("2012-02-26 00:00"),by="60 sec")
pid <- unique(myData$pid)
data_stats <- function(){
  x <- numeric(3064)
  z <- numeric(3064)
  for(i in pid){
    pub_time <- myData[myData$pid == i,]
    bar <- hist(pub_time$date,breaks,plot=FALSE)
    x[i]<-max(bar$counts, na.rm = TRUE)
    z[i]<-i
  }
  a <- data.frame(z,x);
  write.table(file="/home/yash/Downloads/joined click fraud data/timefeature_60secs_feb_23",x=a,sep=",",row.names = FALSE,col.names = FALSE)
}
data_stats()

newTimeFeature <- read.table("/home/yash/Downloads/joined click fraud data/timefeature_60secs_feb_23",sep=",",col.names = c("pid","max"))
newTimeFeature[newTimeFeature==0]<-NA
na.omit(newTimeFeature)
newMyData <- subset(myData,select = c(1,2,3,4,5,6,8,9,10))
partner_iplong<-sqldf("Select pid, count(DISTINCT iplong),count(Distinct agent), count(Distinct cid),count(Distinct cntr),category,count(Distinct referer),status from newMyData group by pid")
total <- merge(partner_iplong,newTimeFeature,by="pid")
newtotal<-total[,c(1,2,3,4,5,6,7,9,8)]
write.table(file = "/home/yash/Downloads/joined click fraud data/counts_with_time_60_feb23",x=newtotal,sep=",",row.names = FALSE,col.names = FALSE)
total <- read.table("/home/yash/Downloads/joined click fraud data/counts_with_time_60_feb23",sep = ",",col.names=c("pid","iplong","agent","cid","cntr","category","referer","max","status"))
newtotal <- total[,c(2,3,4,5,6,7,8,9)]
write.table(file = "/home/yash/Downloads/joined click fraud data/counts_with_time_60_without_partnerid_feb23",x=newtotal,sep=",",row.names = FALSE,col.names = FALSE)