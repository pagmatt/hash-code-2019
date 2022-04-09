#!/usr/bin/env python
from instance import * 
from solution import *
import os
instances_path = './instances/'
solution_path = './solution/'

if __name__ == '__main__':

	overall_score = 0
	instances_fns = os.listdir(instances_path)

	for fn in instances_fns:
		instance = loadInstance(f'{instances_path}{fn}')
		#instance.plot_points_distribution()
		#instance.plot_deadlines_distribution()

		fn_sol = fn.replace ('.in', '.out')
		#solution = loadSolution(f'{solution_path}{fn_sol}', instance)
		#instance_score = evalCheck(instance, solution)
		#print(f'Score = "{instance_score}')
		#overall_score = overall_score + instance_score

	print(f'Overall score = {overall_score}')
