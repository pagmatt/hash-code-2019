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
			instance = loadInstance(f'{path}{fn}')
			solution = solve_instance(instance)

			fn_sol = fn.replace('.in', '.out')
			solution.printSolution(f'{solution_path}{fn_sol}')
			solution_from_file = loadSolution(f'{solution_path}{fn_sol}', instance)
			instance_score = solution_from_file.evalCheck(instance)
			overall_score = overall_score + instance_score

	print(f'\nOverall score = {overall_score}')
