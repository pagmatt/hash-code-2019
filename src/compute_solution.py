#!/usr/bin/env python
from instance import *
from solution import *
import os
from solver import solveInstance
import texttable

instances_paths = ['./instances/', './bigger_instances/']
solution_path = './solution/'

if __name__ == '__main__':

    overall_score = 0
    table = texttable.Texttable()
    table.set_cols_align(["l", "r"])
    rows = [["Instance", "Score"]]
    for path in instances_paths:        
            instances_fns = os.listdir(path)
            for fn in instances_fns:
                #if 'f_big' in fn:
                instance = loadInstance(f'{path}{fn}')
                solution = solveInstance(instance)
                fn_sol = fn.replace('.in', '.out')
                solution.printSolution(f'{solution_path}{fn_sol}')
                solution_from_file = loadSolution(
                    f'{solution_path}{fn_sol}', instance)
                instance_score = solution_from_file.evalCheck(instance)
                rows.append([fn, instance_score])
                overall_score = overall_score + instance_score
    table.add_rows(rows)
    print(table.draw())
    print()
    print(f'\nOverall score = {overall_score}')
