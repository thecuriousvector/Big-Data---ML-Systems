#!/bin/bash
rm -f times.txt
for m in {1..100..10}; do
            for r in {1..100..10}; do
                # time=$(time hadoop jar WordCount.jar WordCount /user/rdv253/book.txt -D mapred.reduce.tasks=$r -D mapred.map.tasks=$m) 
                # echo "$m,$r,$time\n"
		echo -ne "$m,$r," >> times.txt
                /usr/bin/time -f "%e" -a -o times.txt hadoop jar WordCount.jar WordCount /user/rdv253/book.txt /user/rdv253/tempOutputWC -D mapred.reduce.tasks=$r -D mapred.map.tasks=$m
		hadoop fs -rm -r tempOutputWC
         done
done


