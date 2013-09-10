#!/usr/bin/env R

# usage:
# export FILENAME_PREFIX="one_"
# export TITLE_POSTFIX=" [Experiment one]"
# cat 22-experiment-graphs.R | sed s:==FILENAME==:try.db: | R --no-save --quiet

## install.packages("PACKAGE-NAME")
library(ggplot2)
library(RSQLite)
library(scales)
library(reshape2)
library(plyr)

filename_prefix <- Sys.getenv("FILENAME_PREFIX")
title_postfix <- Sys.getenv("TITLE_POSTFIX")
total_message_count <- 2759

# database connection
con <- dbConnect("SQLite", dbname="==FILENAME==")

# dummy data
#filename_prefix <- "FILENAME_PREFIX"
#title_postfix <- "Dummy title postfix [foo, bar, moo, milk]"
#con <- dbConnect("SQLite", dbname="~/all_to_all_largest_3000_try.db")

####################
#  dissemination   #
res <- dbSendQuery(con, statement=paste("SELECT timestamp, peer FROM received_record"))
DATA <- data.frame(fetch(res, n=-1))
DATA <- within(DATA, {received <- ave(timestamp, peer, FUN=seq)})
v <- min(DATA$timestamp)
DATA$timestamp <- apply(DATA, 1, function(x) x[1] - v)
NIL <- sqliteCloseResult(res)

p <- ggplot(DATA)
p <- p + labs(title=bquote(atop("Barter record download progress", atop(italic(.(title_postfix))))))
p <- p + labs(x="Time (seconds)", y="Records received")
p <- p + scale_y_continuous(labels=comma)
#p <- p + annotate("segment", x=0, y=0, xend=total_message_count/(27/5), yend=total_message_count)
p <- p + geom_boxplot(aes(timestamp, received, group=round_any(timestamp, max(timestamp)/30, floor)))
ggsave(filename=paste(filename_prefix, "download_progress.png", sep=""))

# quit
quit(save="no")
