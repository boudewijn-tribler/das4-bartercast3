#compute the # of records that each candidate has at each time step

walk<-read.table("received_record.txt",header=TRUE, col.names=c("viewId","when_received", "when_created", "first", "second", "global_time", "cycle", "upload_first_to_second", "upload_second_to_first", "avg_timestamp"))	


edglist1<-cbind((walk$first+1),(walk$second+1))
G1 <- graph.edgelist(edglist1,directed=TRUE)
G1<-simplify(G1,remove.loops=TRUE,remove.multiple=TRUE)
deg<-degree(G1, v=V(G1), mode = c("all"), loops = FALSE, normalized = FALSE) 


#walk<-read.table("walk_time_default.txt",header=TRUE)	
k<-1
numedge<-c()
n_edg<-c()
n_edg2<-c()

max_tim<-(max(walk$when_received)-min(walk$when_received))/5
tim<-1:max_tim #sort(unique(K1$when_received))


#walk$time_step<-as.integer((walk$when_received-min(walk$when_received))/5.0 +1)

#x<-lapply(split(walk, walk$peer), summary

for (j in tim){
	ned<-rep(0,500)
	for (i in 1:500){

		l<-which(walk$peer==i & walk$when_received<=(j*5+min(walk$when_received)))
		numedge[i]<-length(l)

		}
		n_edg2<-cbind(n_edg2, numedge)
		k<-k+1
		}
		
		
#write.table(n_edg2,"records_per_candidate.txt")
#numedge<-read.table("records_per_candidate.txt")			
no_Nodes<-length(n_edg2[,1])

postscript(file="/var/scratch/dgkorou/records_per_deter_candidate.eps")
#par(cex.lab=1.6,cex.axis=1.3)
p<-plot(unlist(n_edg2[1,]),ylim=c(0,2800),xlim=c(0,length(n_edg2[1,])),ylab="no of records", xlab="time step",main="Dispersy 1 h")
for (i in 2:(no_Nodes-1)){
p<-p + points(unlist(n_edg2[i,]),col=i) }		

print(p)
dev.off()


###number of walks visting a candidate 

K1 <- read.table("walks.txt",header = TRUE, sep = "", 
                col.names=c("fromId", "toId","w"))

#x<-lapply(split(walk, K1$toId), sum)
no_walks<-c()
for(i in 1:no_Nodes){
	l<-which(K1$toId==i)
	 no_walks[i]<-sum(K1$w[l])

	
}


postscript(file="/var/scratch/dgkorou/walks_per_deter_candidate.eps")
#par(cex.lab=1.6,cex.axis=1.3)
p<-plot(no_walks,ylab="no of walks", xlab="candidate Id",main="Dispersy 1 h")
#ylim=c(0,2800),xlim=c(0,length(n_edg2[1,])),
print(p)
dev.off()
 
###number of walks visting a candidate vs its degree


 
edglist1<-cbind((walk$fist+1),(walk$second+1))
G1 <- graph.edgelist(edglist1,directed=TRUE)
G1<-simplify(G1,remove.loops=TRUE,remove.multiple=TRUE)
deg<-degree(G1, v=V(G1), mode = c("all"), loops = FALSE, normalized = FALSE) 



postscript(file="/var/scratch/dgkorou/walks_per_deter_candidate.eps")
#par(cex.lab=1.6,cex.axis=1.3)
p<-plot(no_walks,deg,ylab="no of walks", xlab="degree",main="Dispersy 1 h")
#ylim=c(0,2800),xlim=c(0,length(n_edg2[1,])),
print(p)
dev.off()
 

