#!/usr/bin/env bash                                                                                                                                                                                                                                                                                                            
  

for file in `ls config*`
do 
	./run.sh $file
	sleep 2m
done

echo "all Done."
