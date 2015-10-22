A <-read.table("/home/yash/Downloads/joined click fraud data/feb_23_joined_without_partnerid.csv",sep=",",col.names=c("iplong", "agent", "cid", "cntr", "category", "referer", "status"))
library(DMwR)
A$status <- as.factor(A$status)
A <- SMOTE(status ~ ., A, perc.over = 200, perc.under=200)
A$status <- as.numeric(levels(A$status)[A$status])
A$iplong<-as.integer(A$iplong)
A$agent<-as.integer(A$agent)
#A$partnerid<-as.integer(A$partnerid)
A$cntr<-as.integer(A$cntr)
A$cid<-as.integer(A$cid)
A$category<-as.integer(A$category)
A$referer<-as.integer(A$referer)

#A<-na.omit(A)


print(prop.table(table(A$status)))
write.table(file="/home/yash/Downloads/joined click fraud data/feb_23_joined_without_partnerid_smote.csv",x=A,sep=",",row.names = FALSE,col.names = FALSE)

