library(igraph)
graphs <- list()
graphs[[1]] <- read.graph("graph.txt", format="edgelist")
lay <- lapply(graphs, layout.fruchterman.reingold, niter=30000)

png(file="frplots.png")
par(mai=c(0,0,0,0))
layout(matrix(1:16, nr=4, byrow=TRUE))
layout.show(1)
for (i in seq(along=graphs)) {
  plot(graphs[[i]], layout=lay[[i]],
       vertex.label=NA, vertex.size=13, edge.color="black",
       vertex.color="red")
}
dev.off()
