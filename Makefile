schedSim: schedsim.py
	chmod +x $<
	./$<

clean:
	rm -f ./schedSim