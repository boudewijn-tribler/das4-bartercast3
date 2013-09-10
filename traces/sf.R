

SG_evol <- function(nt,pc){
m<-3
ed<-c()

SGraph<-barabasi.game(10, power = 1, m, out.dist = NULL, out.seq = NULL,out.pref = FALSE, zero.appeal = 1, directed = TRUE)#, time.window = NULL) 
SGraph <- set.edge.attribute(SGraph,"weights", index=E(SGraph),c(1))
E(SGraph )$time<-c(1:ecount(SGraph))
n<-10
step<-ecount(SGraph)
#ntel<-nt-n
#nn<-round(n*p)
while (n<nt){
	t<-runif(1,0,1)
	vc<-c(1)
	d<-degree(SGraph)
	if (t<=pc){
		#add 1 vertex
		SGraph<-add.vertices(SGraph, 1)
		n<-n+1
		l=1
		while (l<=m){
			vc<-sample(1:(vcount(SGraph)-1),1) #random selection of node
			t2<-runif(1,0,1)
			if (t2<(d[vc]/sum(d))){

				edges<-c(vcount(SGraph),vc)
				SGraph<-add.edges(SGraph,edges,"time"=step)
				step<-step+1
				l=l+1
				
			}}}
	else{
		#add m edges
		l=1
		while (l<=m){
			vc<-sample(1:vcount(SGraph),2) #random selection of node
			vc1<-vc[1]
			vc2<-vc[2]
			t2<-runif(1,0,1)
			if (t2<(d[vc1]/sum(d)) & t2<(d[vc2]/sum(d))){
				l=l+1
				edges<-c(vc1,vc2)
				SGraph<-add.edges(SGraph,edges,"time"=step)
				step<-step+1
			#	if ( are.connected(SGraph,vc1,vc2)==TRUE ){
			#		E(SGraph)[(vc1)%--%(vc2)]$weights=E(SGraph)[(vc1)%--%(vc2)]$weights+1}
			#	else {SGraph<-add.edges(SGraph,edges,"weights"=1)}
			}}}
}


return(SGraph)

}

RG_evol <- function(nt,p,pc){

pr=0.02
ed<-c()
n<-10
RanGraph <- erdos.renyi.game(n, pr, type=c("gnp"), directed = TRUE, loops = FALSE)
RanGraph <- set.edge.attribute(RanGraph,"weights", index=E(RanGraph),c(1))
E(RanGraph )$time<-c(1:ecount(RanGraph))


#k<-nn
step<-ecount(RanGraph)
#ntel<-nt-n
while (n<nt){
	nn<-round(n*p)
	if (nn<1){nn<-1}
	#reputation<-get.vertex.attribute(RGraph,"rep",index=V(RGraph))	

	t<-runif(1,0,1)
	if (t<=pc){
		#add 1 vertex
		vc<-sample(1:vcount(RanGraph),nn) #random selection of node
		RanGraph<-add.vertices(RanGraph, 1)
		ed[1:nn]<-c(vcount(RanGraph))
		vc<-vc
		edges<-cbind(ed,vc)
		RanGraph<-add.edges(RanGraph,edges,"time"=step)
		step<-step+1

		n=n+1
		}
	if (t>pc){
		#add pn edges
		vc1<-sample(1:vcount(RanGraph),nn) #random selection of node
		vc1<-vc1
		vc2<-sample(1:vcount(RanGraph),nn) #random selection of node
		vc2<-vc2
		edges<-cbind(vc1,vc2)
		RanGraph<-add.edges(RanGraph,edges,"time"=step)
		step<-step+1
#		if (are.connected(RanGraph,vc1,vc2)==TRUE ){
#			E(RanGraph)[(vc1)%--%(vc2)]$weights=E(RanGraph)[(vc1)%--%(vc2)]$weights+1}
#		else {RanGraph<-add.edges(RanGraph,edges,"weights"=1)}
		}

}

return(RanGraph)

}


