#!/usr/bin/env python
from instance import * 
from solution import *
import sys

if __name__ == '__main__':
	
	instance = loadInstance(sys.argv[1])
	#instance.log()
	solution = loadSolution(sys.argv[2], instance)
	#solution.log()
	print("Score =", evalCheck(instance, solution))
