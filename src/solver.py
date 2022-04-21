import copy
from instance import *
from solution import *
from itertools import product, chain
from mip import *
from progress import *

N_FILES_THRESHOLD = 1
MAX_SEC_OVERALL = 50  # secods
MAX_SEC_SAME_INCUMBENT = 20  # seconds


def optimally_solve_sub_instance(sub_instance: SubInstance, init_solution: Solution = None):
    """
    This class solves in an optimal manner a sub-instance of the problem 
    by formulating it as a MIP problem and the solving it with the Python-MIP library.

    Args:
        sub_instance (SubInstance): the sub-instance to be solved
    """

    # create the MIP model
    model = Model('SingleTargetSubproblem')
    s = sub_instance.nservers   # number of servers
    f = len(sub_instance.filesList)  # numbers of files to compile

    # dummy variable representing the compilation time of the target
    z = model.add_var(name="z")
    # binary variables indicating WHETHER to schedule file f on server s
    x = [[model.add_var(var_type=BINARY, name='x({},{})'.format(j+1, i+1))
          for i in range(s)] for j in range(f)]   # inverse order
    # variables indicating WHEN to schedule file f on server s
    t = [[model.add_var(name='t({},{})'.format(j+1, i+1))
          for i in range(s)] for j in range(f)]
    # dummy variable representing whether t_{f, s} < t_{f', s}
    y = [[model.add_var(var_type=BINARY, name='y({},{})'.format(i+1, j+1))
          for j in range(f)] for i in range(f)]

    bigM = 1.5 * (sum([file.ctime for file in sub_instance.filesList])
                  + sum([file.rtime for file in sub_instance.filesList]))

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
                ctime, _, idx = sub_instance.get_times_and_idx(dep)
                model += t[j][i] >= t[idx][i] + ctime - \
                    bigM*(2 - x[j][i] - x[idx][i])

    # dependency constraint on different server
    for j in range(f):
        deps = sub_instance.filesList[j].dependencies
        for dep in deps:
            for (i, k) in product(range(s), range(s)):
                if (i != k):  # if different servers
                    ctime, rtime, idx = sub_instance.get_times_and_idx(dep)
                    model += t[j][i] >= t[idx][k] + ctime + \
                        rtime - bigM*(2 - x[j][i] - x[idx][k])

    # all files must be compiled
    for i in range(f):
        model += xsum(x[i][j] for j in range(s)) >= 1

    # non-concurrent compilation, case t_{f, s} >= t_{f', s}
    for (j, k) in product(range(f), range(f)):
        if (j != k):
            for i in range(s):
                model += t[j][i] >= t[k][i] + sub_instance.filesList[k].ctime - \
                    bigM*(3 - x[j][i] - x[k][i] - (1 - y[j][k]))

    # non-concurrent compilation, case t_{f, s} < t_{f', s}
    for (j, k) in product(range(f), range(f)):
        if (j != k):
            for i in range(s):
                model += t[k][i] >= t[j][i] + sub_instance.filesList[j].ctime - \
                    bigM*(3 - x[j][i] - x[k][i] - y[j][k])

    if(init_solution is not None):
        # set initial feasible solution to speedup the B&C algorithm
        x_start = [[0] * f for _ in range(s)]
        t_start = [[0] * f for _ in range(s)]
        y_start = [[0] * f for _ in range(f)]

        # set values for x variables
        for s_idx in range(s):
            for step in init_solution.compSteps[s_idx]:
                ctime, _, f_idx = sub_instance.get_times_and_idx(step)
                x_start[s_idx][f_idx] = 1
                t_start[s_idx][f_idx] = init_solution.filesAvailTime[s_idx][step] - ctime

            for (i, j) in product(range(f), range(f)):
                if (i != j and x_start[s_idx][i] == 1 and x_start[s_idx][j] == 1):
                    cond = t_start[s_idx][i] > t_start[s_idx][j]
                    y_start[i][j] = int(cond)

        x_start_vars = [(x[k][j], x_start[j][k])
                        for (j, k) in product(range(s), range(f))]
        y_start_vars = [(y[k][j], y_start[j][k])
                        for (j, k) in product(range(f), range(f))]
        model.start = x_start_vars + y_start_vars

    model.verbose = 0
    model.objective = z
    status = model.optimize(max_seconds_same_incumbent=MAX_SEC_SAME_INCUMBENT,
                            max_seconds=MAX_SEC_OVERALL)  # set a worst-case limit to the solver runtime
    found = False
    obj = 0

    if (status == OptimizationStatus.OPTIMAL or
            status == OptimizationStatus.FEASIBLE):
        found = True
        obj = int(z.x)

        # output the solution
        # solution = Solution(s)

        # for (j, i) in product(range(f), range(s)):
        #     if (x[j][i].x >= 0.99):
        #         print("compilation %d starts on server %d at time %g " % (j+1, i+1, t[j][i].x))

        # build a solution from the results of the solvers

    return [found, obj]


def heuristically_solve_sub_instance(sub_instance: SubInstance):
    """
    This class solves via an ad-hoc heuristic a sub-instance of the problem.
    Since it is (in principle) sub-optimal, it is used only whenever the sub-instance is too big
    to be solved exactly or as a starting solution for the branch & cut algorithm.

    Args:
        sub_instance (SubInstance): the sub-instance to be solved.
    """
    heuristic_sol = Solution(sub_instance.nservers)

    for file in sub_instance.filesList:
        earliest_s = heuristic_sol.get_earliest_server_for_file(
            file.name, sub_instance)
        heuristic_sol.add_step(file.name, earliest_s, sub_instance)


    #plot a sketch of the subproblem result

    # from matplotlib import pyplot as plt
    # fig, ax = plt.subplots()
    # for s in range(heuristic_sol.nservers):
    #     for f in heuristic_sol.compSteps[s]:
    #         ax.barh(s, width=sub_instance.filesDict[f].ctime, left=heuristic_sol.filesAvailTime[s][f] - sub_instance.filesDict[f].ctime, alpha=0.5)
    # plt.show()

    return heuristic_sol


def merge_sub_instances(sub_inst_a: SubInstance, sol_a: Solution, sub_inst_b: SubInstance, sol_b: Solution):
    """
    This class marges the solutions of two sub-instances into a single solution

    Args:
        sub_inst_a (SubInstance): the first sub-instance.
        sol_a (Solution): the solution of the first sub-instance.
        sub_inst_b (SubInstance): the second sub-instance.
        sol_b (Solution): the solution of the second sub-instance.
    """

    assert(sub_inst_a.nservers == sub_inst_b.nservers
           == sol_a.nservers == sol_b.nservers)
    s = sub_inst_a.nservers
    tf = sub_inst_b.filesDict[sub_inst_b.target]
    sol_a_old = copy.deepcopy(sol_a)

    for sched_file in sol_b.filesCompTime:
        flat_steps = list(chain(*sol_a.compSteps))
        if(sched_file.fname not in flat_steps):
            earliest_s = sol_a.get_earliest_server_for_file(sched_file.fname, sub_inst_b)
            sol_a.add_step(sched_file.fname, earliest_s, sub_inst_b)

    # did we manage to compile the target in time? if not, remove the new compilations
    t_aval_time = min([sol_a.filesAvailTime[i][tf.name] for i in range(s)])
    if (t_aval_time > sub_inst_b.get_deadline()):
        sol_a = sol_a_old

    return sol_a


def rec_load_dependencies(instance: Instance, file: CompiledFile):
    dependencies = []
    for dep in instance.files[file].dependencies:
        dependencies.append(instance.files[dep])
        if (len(instance.files[dep].dependencies) > 0):
            dependencies.extend(rec_load_dependencies(instance, dep))

    return dependencies


def solve_instance(instance: Instance):
    """
    This class solves an instance of the Hash Code 2019 final problem by splitting the original problem
    into multiple subproblems, each comprising a single target file.
    The subproblems are solved via a MIP optimization whenever the problem size is not too big. In the latter case,
    an heuristic is used. 
    Finally, the solutions of the subproblems are merged into a single solution.

    Args:
        instance (Instance): the Hash Code 2019 final instance to solve
    """

    num_targets = len(instance.targets)
    sub_sol, sub_inst = [], []
    scores = [0] * num_targets
    progress(0, num_targets*2, '')
    counter = 0
    delta = []

    for target in instance.targets:

        assert(target in instance.files)

        progress(counter, num_targets*2, f'{instance.name} - solving subinstances')

        # create sub-problem
        relevant_files_list = [instance.files[target]]
        dependencies = rec_load_dependencies(instance, target)
        relevant_files_list.extend(dependencies)
        relevant_files_list.reverse()   # dependencies first
        unique_file_list = []
        for elem in relevant_files_list:
            already_there = [file.name for file in unique_file_list]
            if(elem.name not in already_there):
                unique_file_list.append(elem)
        relevant_files_dict = {}
        for file in unique_file_list:
            relevant_files_dict[file.name] = file
        # print(f'Solving subinstance of size: {len(relevant_files_list)} and nservers: {instance.nservers}')

        sub_problem = SubInstance(
            unique_file_list, relevant_files_dict, target, instance.nservers)
        heuristic_solution = heuristically_solve_sub_instance(sub_problem)
        if (len(unique_file_list) < N_FILES_THRESHOLD):
            [found, obj] = optimally_solve_sub_instance(
                sub_problem, heuristic_solution)
            if found:
                tf = sub_problem.target
                t_aval_time = min([heuristic_solution.filesAvailTime[j][tf]
                                for j in range(sub_problem.nservers)])
                delta.append(t_aval_time - obj)
            else:
                delta.append(0)

        #check no overlapping compilations
        for server in range(instance.nservers):
            time = 0
            for step in heuristic_solution.compSteps[server]:
                stime = heuristic_solution.getSchedTime(step, server)
                ctime = sub_problem.filesDict[step].ctime
                if (stime < time):
                    print(step)
                assert(stime >= time)
                time = stime + ctime


        sub_inst.append(sub_problem)
        sub_sol.append(heuristic_solution)
        counter = counter + 1

    # sort the targets
    assert(len(sub_sol) == len(sub_inst) == len(scores))
    for i in range(len(sub_sol)):
        tf = sub_inst[i].target
        s = sub_inst[i].nservers
        t_aval_time = min([sub_sol[i].filesAvailTime[j][tf] for j in range(s)])
        deadline = sub_inst[i].get_deadline()
        if (t_aval_time <= deadline):
            scores[i] = deadline - t_aval_time + \
                sub_inst[i].get_compil_points()
    #print(scores)

    # get the indices of the list sorted in descending order
    idxes = np.flip(np.argsort(np.argsort(scores)))

    # solution = sub_sol[idxes[0]]
    # prev_inst = sub_inst[idxes[0]]
    # for i in range(1, len(sub_sol)):
    #     progress(counter, num_targets*2, 'Merging subinstances')
    #     if(scores[idxes[i]] > 0):  # skip subproblems we couldn't solve
    #         solution = merge_sub_instances(
    #             prev_inst, solution, sub_inst[idxes[i]], sub_sol[idxes[i]])
    #         prev_inst = sub_sol[idxes[i]]
    #     counter = counter + 1
    # progress(counter, num_targets*2, 'Solved instance')

    solution = sub_sol[0]
    prev_inst = sub_inst[0]
    counter = counter + 1
    for i in range(1, len(sub_sol)):
        progress(counter, num_targets*2, f'{instance.name} - merging subinstances')
        counter = counter + 1
        if(scores[i] > 0):  # skip subproblems we couldn't solve
            solution = merge_sub_instances(
                prev_inst, solution, sub_inst[i], sub_sol[i])
            prev_inst = sub_sol[i]
    progress(counter, num_targets*2, f'{instance.name} - solved')

    return solution
