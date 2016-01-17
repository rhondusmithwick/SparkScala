#!/bin/bash
i=30
until [ $i -eq 0 ]; do
    echo $i "more programs to go.";
	if [ $((i%2)) -eq 0 ] 
	then
	    echo "Running Wikipedia.scala.";
		sh TerminalCode/Wikipedia.sh;
	else
	    echo "Running WhiteHouse.scala.";
		sh TerminalCode/WhiteHouse.sh;
	fi
	let i-=1;
	sleep 5;
done