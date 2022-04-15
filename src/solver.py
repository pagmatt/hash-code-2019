from numpy import s_
from pyparsing import dblQuotedString
from instance import * 
from solution import *
from itertools import product, chain
from mip import *
N_FILES_THRESHOLD = 0

def optimally_solve_sub_instance (sub_instance: SubInstance, init_solution: Solution = None):
    """
    This class solves in an optimal manner a sub-instance of the problem 
    by formulating it as a MIP problem and the solving it with the Python-MIP library.

    Args:
        sub_instance (SubInstance): the sub-instance to be solved
    """

    # create the MIP model
    model = Model('SingleTargetSubproblem')
    s = sub_instance.nservers   # number of servers
    f = len(sub_instance.filesList) # numbers of files to compile

    # dummy variable representing the compilation time of the target
    z = model.add_var(name="z")
    # binary variables indicating WHETHER to schedule file f on server s
    x = [[model.add_var(var_type=BINARY, name='x({},{})'.format(j+1, i+1))
        for i in range(s)] for j in range(f)]   # inverse order
    # variables indicating WHEN to schedule file f on server s
    t = [[model.add_var(name='t({},{})'.format(j+1, i+1))
        for i in range(s)] for j in range(f)]
    # dummy variable representing whether t_{f, s} < t_{f', s}
    y = [[[model.add_var(var_type=BINARY, name='y({},{},{})'.format(i+1, j+1, k+1))
        for k in range(s)] for j in range(f)] for i in range(f)] 
    
    bigM = 1.5 * (sum ([file.ctime for file in sub_instance.filesList]) 
                + sum ([file.rtime for file in sub_instance.filesList]))

    # constraints to try and reduce the computational load
    model += z <= sub_instance.get_deadline()
    for (i, j) in product(range(f), range(s)):
        model += t[i][j] <= sub_instance.get_deadline()

    # definition of the dummy objective
    for (i, j) in product(range(f), range(s)):
        model += z >= t[i][j] - bigM*(1 - x[i][j])

    # dependency constraint on same server
    for j in range(f):
        deps = sub_instance.filesList[j].dependencies
        for dep in deps:
            for i in range(s):
                ctime, _, idx = sub_instance.get_times_and_idx (dep)
                model += t[j][i] >= t[idx][i] + ctime - bigM*(2 - x[j][i] - x[idx][i])

    # dependency constraint on different server 
    for j in range(f):
        deps = sub_instance.filesList[j].dependencies
        for dep in deps:
            for (i, k) in product(range(s), range(s)):
                if (i != k): # if different servers
                    ctime, rtime, idx = sub_instance.get_times_and_idx (dep)
                    model += t[j][i] >= t[idx][k] + ctime + rtime - bigM*(2 - x[j][i] - x[idx][k])

    # all files must be compiled
    for i in range (f):
        model += xsum(x[i][j] for j in range(s)) >= 1

    # non-concurrent compilation, case t_{f, s} >= t_{f', s}
    for (j, k) in product(range(f), range(f)):
        if (j != k):
            for i in range(s):
                model += t[j][i] >= t[k][i] + sub_instance.filesList[k].ctime - bigM*(3 - x[j][i] - x[k][i] - (1 - y[j][k][i]))

    # non-concurrent compilation, case t_{f, s} < t_{f', s}
    for (j, k) in product(range(f), range(f)):
        if (j != k):    
            for i in range(s):
                model += t[k][i] >= t[j][i] + sub_instance.filesList[j].ctime - bigM*(3 - x[j][i] - x[k][i] - y[j][k][i])

    if(init_solution is not None):
        # set initial feasible solution to speedup the B&C algorithm
        x_start = [[0] * f for _ in range(s)]
        t_start = [[0] * f for _ in range(s)]
        y_start = [[[0] * f for _ in range(f)] for _ in range(s)]

        # set values for x variables
        for s_idx in range(s):
            for step in init_solution.compSteps[s_idx]:
                ctime, _, f_idx = sub_instance.get_times_and_idx(step)
                x_start[s_idx][f_idx] = 1
                t_start[s_idx][f_idx] = init_solution.filesAvailTime[s_idx][step] - ctime
        
            for (i, j) in product(range(f), range(f)):
                if (i != j and x_start[s_idx][i] == 1 and x_start[s_idx][j] == 1):
                    cond = t_start[s_idx][i] > t_start[s_idx][j]
                    y_start[s_idx][i][j] = int(cond)
        
        #model.start = [(x[k][j], x_start[j][k]) for (j, k) in product(range(s), range(f))]
        x_start_vars = [(x[k][j], x_start[j][k]) for (j, k) in product(range(s), range(f))]
        y_start_vars = [(y[l][k][j], y_start[j][k][l]) for (j, k, l) in product(range(s), range(f), range(f))]
        model.start = x_start_vars + y_start_vars
        #model.start = [[(t[k][j], t_start[j][k]) for j in range(s)] for k in range(f)]

    model.objective = z
    status = model.optimize(max_seconds_same_incumbent=10, max_seconds=50)  # set a worst-case limit to the solver runtime

    if (status == OptimizationStatus.OPTIMAL or 
            status == OptimizationStatus.FEASIBLE):
        print("Completion time: ", z.x)
        for (j, i) in product(range(f), range(s)):
            if (x[j][i].x >= 0.99):
                print("compilation %d starts on server %d at time %g " % (j+1, i+1, t[j][i].x))

        # from matplotlib import pyplot as plt

        # #plot a sketch of the subproblem result
        # fig, ax = plt.subplots()
        # for (j, i) in product(range(f), range(s)):
        #     if (x[j][i].x >= 0.99):
        #         ax.barh(i, width=sub_instance.filesList[j].ctime, left=t[j][i].x)
        # plt.show()

    return Solution (sub_instance.nservers)

def heuristically_solve_sub_instance(sub_instance: SubInstance):
    """
    This class solves via an ad-hoc heuristic a sub-instance of the problem.
    Since it is (in principle) sub-optimal, it is used only whenever the sub-instance is too big
    to be solved exactly or as a starting solution for the branch & cut algorithm.

    Args:
        sub_instance (SubInstance): the sub-instance to be solved.
    """
    heuristic_sol = Solution(sub_instance.nservers)

    for file in reversed(sub_instance.filesList):
        earliest_s = heuristic_sol.get_earliest_server_for_file(file.name, sub_instance)
        heuristic_sol.add_step(file.name, earliest_s, sub_instance)
	    
    return heuristic_sol

def merge_sub_instances(sub_inst_a: SubInstance, sol_a: Solution, sub_inst_b: SubInstance, sol_b: Solution):

    assert(sub_inst_a.nservers == sub_inst_b.nservers \
            == sol_a.nservers == sol_b.nservers)
    s = sub_inst_a.nservers
    tf = sub_inst_b.filesDict[sub_inst_b.target]
    new_f = []

    for sched_file in sol_b.filesCompTime:
        flat_steps = list(chain(*sol_a.compSteps))
        if(sched_file.fname not in flat_steps):
            earliest_s = sol_a.get_earliest_server_for_file(sched_file.fname, sub_inst_b)
            sol_a.add_step(sched_file.fname, earliest_s, sub_inst_b)
            new_f.append(sched_file.fname)

    # did we manage to compile the target in time? if not, remove the new compilations
    

    #TODO: remove if can not meet deadline

    return sol_a

def rec_load_dependencies(instance: Instance, file: CompiledFile):
    dependencies = []
    for dep in instance.files[file].dependencies:
        dependencies.append(instance.files[dep])
        if (len(instance.files[dep].dependencies) > 0):
            dependencies.extend (rec_load_dependencies (instance, dep))
    
    return dependencies

def solve_instance(instance: Instance):
    # solve sub-instances
    solution = Solution(instance.nservers)
    prevInst = None

    for target in instance.targets:
        assert(target in instance.files)

        # create sub-problem
        relevant_files_list = [instance.files[target]]
        dependencies = rec_load_dependencies(instance, target)
        relevant_files_list.extend(dependencies)
        relevant_files_dict = {}
        for file in relevant_files_list:
            relevant_files_dict[file.name] = file
        # print(f'Solving subinstance of size: {len(relevant_files_list)} and nservers: {instance.nservers}')

        sub_problem = SubInstance(relevant_files_list, relevant_files_dict, target, instance.nservers)
        heuristic_solution = heuristically_solve_sub_instance(sub_problem)
        #if (len(relevant_files_list) < N_FILES_THRESHOLD):  
        #    optimally_solve_sub_instance(sub_problem, heuristic_solution)

        if(prevInst != None):
            solution = merge_sub_instances(prevInst, solution, sub_problem, heuristic_solution)
        else:
            solution = heuristic_solution
        prevInst = sub_problem

    return solution