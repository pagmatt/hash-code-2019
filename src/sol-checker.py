#!/usr/bin/env python
from instance import * 
from solution import *
import os
from solver import solve_instance

instances_paths = ['./instances/', './bigger_instances/']

solution_path = './solution/'

if __name__ == '__main__':

	overall_score = 0
	for path in instances_paths:
		instances_fns = os.listdir(path)

		for fn in instances_fns:
			print(f'Solving instance: {fn}')
			instance = loadInstance(f'{path}{fn}')
			solution = solve_instance (instance)

			#instance.plot_points_distribution()
			#instance.plot_deadlines_distribution()

			#fn_sol = fn.replace ('.in', '.out')
			#solution = loadSolution(f'{solution_path}{fn_sol}', instance)
			#instance_score = solution.evalCheck(instance)
			#print(f'Score = "{instance_score}')
			#overall_score = overall_score + instance_score

	print(f'Overall score = {overall_score}')
