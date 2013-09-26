library(igraph)

source("sf.R")
library("RSQLite")


sqlite    <- dbDriver("SQLite")

nt<-500
pc<-0.5

sf<-SG_evol(nt,pc)
sf<-as.undirected(sf)

sf<-simplify(sf,remove.loops=TRUE,remove.multiple=TRUE)
E(sf)$weights1<-runif(ecount(sf),0,9000)
E(sf)$weights2<-runif(ecount(sf),0,9000)

t<-get.edgelist(sf)
global_time<-c()
cycle<-c()
effort<-c()
for (i in 1:ecount(sf)){
	global_time<-cbind(global_time,(3+i))
	cycle<-cbind(cycle,998+i)
	}
global_time<-as.vector(global_time)
cycle<-as.vector(cycle)

for (i in 1:ecount(sf)){
	effort<-cbind(effort,1)}

effort<-as.vector(effort)
t[,1]<-as.integer(t[,1])
t[,2]<-as.integer(t[,2])

tabl<-cbind(t,global_time,cycle,effort,E(sf)$weights1,E(sf)$weights2)

colnames(tabl)<-c("first_peer_number","second_peer_number","global_time","cycle","effort","upload_first_to_second","upload_second_to_first")
tabl[,1]<-tabl[,1]-1
tabl[,2]<-tabl[,2]-1

tabl<-as.data.frame(tabl)
tabl[,1]<-as.integer(tabl[,1])
tabl[,2]<-as.integer(tabl[,2])
tabl[,3]<-as.integer(tabl[,3])
tabl[,4]<-as.integer(tabl[,4])
tabl[,5]<-as.integer(tabl[,5])
tabl[,6]<-as.integer(tabl[,6])
tabl[,7]<-as.integer(tabl[,7])

mydb <- dbConnect(sqlite,"500peers.db")
# CREATE TABLE predefined_records(
#   first_peer_number INT,second_peer_number INT, global_time INT, cycle INT,upload_first_to_second INT,
#   upload_second_to_first INT);
dbWriteTable(mydb,"predefined_records",tabl,row.names = FALSE)

dbWriteTable(mydb,"predefined_books",tabl,row.names = FALSE)

dbDisconnect(mydb)
