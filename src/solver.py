from turtle import right
from instance import * 
from solution import *
from itertools import product
from mip import *
N_FILES_THRESHOLD = 100

def optimally_solve_sub_instance (sub_instance: SubInstance):
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
        deps = sub_instance.files[j].dependencies
        for dep in deps:
            for i in range(s):
                ctime, rtime, idx = sub_instance.get_times_and_idx (dep)
                model += t[j][i] >= t[idx][i] + ctime - bigM*(2 - x[j][i] - x[idx][i])

    # dependency constraint on different server 
    for j in range(f):
        deps = sub_instance.files[j].dependencies
        for dep in deps:
            for (i, k) in product(range(s), range(s)):
                if (i != k): # if different servers
                    ctime, rtime, idx = sub_instance.get_times_and_idx (dep)
                    model += t[j][i] >= t[idx][k] + ctime + rtime - bigM*(2 - x[j][i] - x[idx][k])

    # all files must be compiled
    for i in range (f):
        model += xsum(x[i][j] for j in range(s)) >= 1

    # non-concurrent compilation, case t_{f, s} >= t_{f, s'}
    for (j, k) in product(range(f), range(f)):
        if (j != k):
            for i in range(s):
                model += t[j][i] >= t[k][i] + sub_instance.filesList[k].ctime - bigM*(3 - x[j][i] - x[k][i] - (1 - y[j][k][i]))

    # non-concurrent compilation, case t_{f, s} < t_{f, s'}
    for (j, k) in product(range(f), range(f)):
        if (j != k):
            for i in range(s):
                model += t[k][i] >= t[j][i] + sub_instance.filesList[j].ctime - bigM*(3 - x[j][i] - x[k][i] - y[j][k][i])

    model.objective = z
    status = model.optimize(max_seconds_same_incumbent=30)  # set a worst-case limit to the solver runtime

    assert (status == OptimizationStatus.OPTIMAL or 
            status == OptimizationStatus.FEASIBLE)
    print("Completion time: ", z.x)
    for (j, i) in product(range(f), range(s)):
        if (x[j][i].x >= 0.99):
            print("compilation %d starts on server %d at time %g " % (j+1, i+1, t[j][i].x))

    from matplotlib import pyplot as plt

    # plot a sketch of the subproblem result
    # fig, ax = plt.subplots()
    # for (j, i) in product(range(f), range(s)):
    #     if (x[j][i].x >= 0.99):
    #         ax.barh(i, width=sub_instance.files[j].ctime, left=t[j][i].x)
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

    from matplotlib import pyplot as plt

    #plot a sketch of the subproblem result
    fig, ax = plt.subplots()
    for s in range(heuristic_sol.nservers):
        for f in heuristic_sol.files[s].keys():
            ax.barh(s, width=sub_instance.filesDict[f].ctime, left=heuristic_sol.files[s][f] - sub_instance.filesDict[f].ctime)
    plt.show()
	    
    return heuristic_sol

def rec_load_dependencies(instance: Instance, file: CompiledFile):
    dependencies = []
    for dep in instance.files[file].dependencies:
        dependencies.append(instance.files[dep])
        if (len(instance.files[dep].dependencies) > 0):
            dependencies.extend (rec_load_dependencies (instance, dep))
    
    return dependencies

def solve_instance(instance: Instance):
    # solve sub-instances
    for target in instance.targets:

        assert(target in instance.files)

        # create sub-problem
        relevant_files_list = [instance.files[target]]
        dependencies = rec_load_dependencies(instance, target)
        relevant_files_list.extend(dependencies)
        relevant_files_dict = {}
        for file in relevant_files_list:
            relevant_files_dict[file.name] = file
        print(f'Solving subinstance of size: {len(relevant_files_list)}')

        sub_problem = SubInstance(relevant_files_list, relevant_files_dict, target, instance.nservers)
        heuristic_solution = heuristically_solve_sub_instance(sub_problem)
        #if (N_FILES_THRESHOLD < N_FILES_THRESHOLD):  
        #    len(relevant_files_list)(sub_problem)

if __name__ == '__main__':

    # plt.show ()
    print('nope, wrong file :D')