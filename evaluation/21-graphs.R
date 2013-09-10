#!/usr/bin/env R

# usage:
# export FILENAME_PREFIX="one_"
# export TITLE_POSTFIX=" [Experiment one]"
# cat 21-graphs.R | sed s:==FILENAME==:try.db: | R --no-save --quiet

## install.packages("PACKAGE-NAME")
library(ggplot2)
library(RSQLite)
library(scales)
library(reshape2)
library(plyr)

filename_prefix <- Sys.getenv("FILENAME_PREFIX")
title_postfix <- Sys.getenv("TITLE_POSTFIX")

# database connection
con <- dbConnect("SQLite", dbname="==FILENAME==")

# dummy data
#filename_prefix <- "FILENAME_PREFIX"
#title_postfix <- "Dummy title postfix [foo, bar, moo, milk]"
#con <- dbConnect("SQLite", dbname="~/all_to_all_largest_3000_try.db")

####################
#       CPU        #
res <- dbSendQuery(con, statement=paste("SELECT peer.hostname AS hostname, cpu.timestamp AS timestamp, cpu.percentage / 100.0 AS percentage FROM cpu JOIN peer ON peer.id = cpu.peer"))
DATA <- data.frame(fetch(res, n=-1))
NIL <- sqliteCloseResult(res)

p <- ggplot(DATA, aes(timestamp, percentage, color=hostname))
p <- p + labs(title=bquote(atop("CPU usage", atop(italic(.(title_postfix))))))
p <- p + labs(x="Time (seconds)", y="Usage (percentage)")
p <- p + geom_point(shape=".")
p <- p + geom_smooth(method="auto")
p <- p + scale_y_continuous(labels=percent)
p
ggsave(filename=paste(filename_prefix, "cpu.png", sep=""))

####################
#      memory      #
res <- dbSendQuery(con, statement=paste("SELECT timestamp, rss, vms FROM memory"))
DATA <- data.frame(fetch(res, n=-1))
NIL <- sqliteCloseResult(res)

p <- ggplot(DATA, aes(timestamp, vms))
p <- p + labs(title=bquote(atop("Virtual Memory Size", atop(italic(.(title_postfix))))))
p <- p + labs(x="Time (seconds)", y="Usage (bytes)")
p <- p + geom_point(shape=".")
p <- p + geom_smooth(method="auto")
p <- p + scale_y_continuous(labels=comma)
p
ggsave(filename=paste(filename_prefix, "memory.png", sep=""))

####################
#     bandwidth    #
res <- dbSendQuery(con, statement=paste("SELECT timestamp, up, down FROM bandwidth_rate"))
DATA <- data.frame(fetch(res, n=-1))
DATA <- melt(DATA, id=c("timestamp"))
NIL <- sqliteCloseResult(res)

p <- ggplot(DATA, aes(timestamp, value, color=variable))
p <- p + labs(title=bquote(atop("Bandwidth rate", atop(italic(.(title_postfix))))))
p <- p + labs(x="Time (seconds)", y="Usage (bytes)")
p <- p + geom_point(shape=".")
p <- p + geom_smooth(method="auto")
p <- p + scale_y_continuous(labels=comma)
p
ggsave(filename=paste(filename_prefix, "bandwidth.png", sep=""))

###########################
# packet success and drop #
res <- dbSendQuery(con, statement=paste("SELECT timestamp, drop_count, delay_count, delay_send, delay_success, delay_timeout, success_count, received_count FROM bandwidth"))
DATA <- data.frame(fetch(res, n=-1))
DATA <- melt(DATA, id=c("timestamp"))
NIL <- sqliteCloseResult(res)

p <- ggplot(DATA, aes(timestamp, value, color=variable))
p <- p + labs(title=bquote(atop("Ability to process received packets", atop(italic(.(title_postfix))))))
p <- p + labs(x="Time (seconds)", y="Counters")
p <- p + geom_point(shape=".")
p <- p + geom_smooth(method="auto")
p <- p + scale_y_continuous(labels=comma)
p
ggsave(filename=paste(filename_prefix, "success_and_loss.png", sep=""))

####################
#      churn       #
res <- dbSendQuery(con, statement=paste("SELECT online, offline FROM churn"))
TABLE <- fetch(res, n=-1)
DATA <- data.frame(timestamp=TABLE$online, value=1)
DATA <- rbind(DATA, data.frame(timestamp=TABLE$offline, value=-1))
sqliteCloseResult(res)

DATA <- DATA[order(DATA$timestamp),]
DATA$value <- cumsum(DATA$value)

p <- ggplot(DATA, aes(timestamp, value))
p <- p + labs(title=bquote(atop("Churn", atop(italic(.(title_postfix))))))
p <- p + geom_step()
p <- p + scale_y_continuous(labels=comma)
p
ggsave(filename=paste(filename_prefix, "churn.png", sep=""))

# quit
quit(save="no")
