from instance import * 
from solution import *
from itertools import product
from mip import *

def solve_instance (instance):

    return Solution ()

if __name__ == '__main__':

    # create the MIP model
    model = Model('HashCodeSingleTargetSubproblem')
    s = 3
    f = 9
    d = [[],    # dep.cies of 1st file
         [0],   # dep of 2nd file
         [0],
         [1, 2],
         [1],
         [3],
         [2],
         [2, 4],
         [7]
         ]
    c = [3, 4, 5, 1, 6, 3, 8, 5, 4] # compilation times
    r = [9, 0, 2, 3, 4, 5, 7, 2, 1] # replication times

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
    
    bigM = 2 * (sum (c) + sum (r)) # TODO:temp

    # definition of the dummy objective
    for (i, j) in product(range(f), range(s)):
        model += z >= t[i][j] - bigM*(1 - x[i][j])

    # dependency constraint on same server
    for j in range(f):
        deps = d[j]
        for dep in deps:
            for i in range(s):
                model += t[j][i] >= t[dep][i] + c[dep] - bigM*(2 - x[j][i] - x[dep][i])

    # dependency constraint on different server 
    for j in range(f):
        deps = d[j]
        for dep in deps:
            for (i, k) in product(range(s), range(s)):
                if (i != k): # if different servers
                    model += t[j][i] >= t[dep][k] + c[dep] + r[dep] - bigM*(2 - x[j][i] - x[dep][k])

    # all files must be compiled
    for i in range (f):
        model += xsum(x[i][j] for j in range(s)) >= 1

    # non-concurrent compilation, case a
    for (j, k) in product(range(f), range(f)):
        if (j != k):
            for i in range(s):
                model += t[j][i] >= t[k][i] + c[k] - bigM*(3 - x[j][i] - x[k][i] - (1 - y[j][k][i]))

    # non-concurrent compilation, case b
    for (j, k) in product(range(f), range(f)):
        if (j != k):
            for i in range(s):
                model += t[k][i] >= t[j][i] + c[j] + r[j] - bigM*(3 - x[j][i] - x[k][i] - y[j][k][i])

    model.objective = z
    status = model.optimize()

    print (status)
    print("Completion time: ", z.x)
    for (j, i) in product(range(f), range(s)):
        if (x[j][i].x >= 0.99):
            print("compilation %d starts on server %d at time %g " % (j+1, i+1, t[j][i].x))