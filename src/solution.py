from collections import deque
from instance import *
import sys


class SchedFile():
    def __init__(self, fname: str, sched_time: int, server: int):
        self.fname = fname
        self.sched_time = sched_time
        self.server = server


class Solution():
    def __init__(self, nservers):
        self.nservers = nservers
        # compilation steps performed at each server. Kept in chronological order
        self.compSteps = [[] for s in range(self.nservers)]
        # times when files are ready at each server
        self.filesAvailTime = [{} for s in range(self.nservers)]
        self.filesCompTimeList = []
        # faster to search compared to the corresponding list, but not sorted time wise
        self.filesCompTimeDict = {}
        # current time at each server ~ last instant during which a file is compiled
        self.currTime = [0 for s in range(self.nservers)]
        # are there gaps between compilation in a given server ?
        self.gaps = [False for s in range(self.nservers)]

    def log(self):
        for s in range(self.nservers):
            print(self.compSteps[s])

    def evalCheck(self, instance) -> int:
        # queue of compilation steps for each server
        queues = [deque(self.compSteps[s]) for s in range(instance.nservers)]
        # current time at each server
        time = [0 for s in range(instance.nservers)]
        # files ready at each server (with corresponding time)
        files = [{} for s in range(instance.nservers)]
        # simulate compilation
        while(True):
            nDone = 0
            for s in range(instance.nservers):
                if not queues[s]:
                    continue
                f = queues[s][0]
                # check if all dependencies are satisfied
                cf = instance.files[f]
                depOk = True
                startTime = time[s]
                for dep in cf.dependencies:
                    if dep not in files[s]:
                        depOk = False
                        break
                    else:
                        startTime = max(startTime, files[s][dep])
                if depOk:
                    #print(f'Execute {cf.name} on server {s} at time {startTime}-{startTime+cf.ctime}')
                    # remove it from queue
                    queues[s].popleft()
                    # we can execute f on s
                    nDone += 1
                    # mark f available (either by compilation or replication)
                    # NOTE: the same file might be compiled multiple times, either on the same server or on multiple servers.
                    #       Thus, we must be careful of not overwriting the time a file is available with a higher value.
                    for otherS in range(instance.nservers):
                        afterCompilation = startTime + cf.ctime
                        afterReplication = afterCompilation + cf.rtime
                        afterTime = afterReplication if otherS != s else afterCompilation
                        if cf.name in files[otherS]:
                            files[otherS][cf.name] = min(
                                files[otherS][cf.name], afterTime)
                        else:
                            files[otherS][cf.name] = afterTime
                    time[s] = startTime + cf.ctime
            if not nDone:
                break
        # if queues are not empty, then we have messed up with dependencies
        for s in range(instance.nservers):
            assert(not queues[s])
        # merge files from all servers
        targets = {}
        for s in range(instance.nservers):
            for f in files[s]:
                if f in targets:
                    targets[f] = min(targets[f], files[s][f])
                else:
                    targets[f] = files[s][f]
        # evaluate targets
        score = 0
        for t in targets:
            cf = instance.files[t]
            if (cf.points > 0) and (targets[t] <= cf.deadline):
                score += (cf.deadline - targets[t]) + cf.points
        return score

    def add_step(self, fname: str, server: int, instance: SubInstance):
        assert(fname in instance.filesDict.keys())

        deps = instance.filesDict[fname].dependencies
        dep_avail_time = self.getDepAvailTime(instance, fname, server)

        # if fname == 'cu':
        #     print(f'cu deps {deps}')
        #     print(f'avail time {dep_avail_time}')

        # schedule more than once, if we would be waiting without doing anything
        not_avail = [f for f in deps if self.filesAvailTime[server][f] > self.currTime[server]]
        can_fit = [f for f in not_avail if self.getEarliestGapToSched( \
            instance, server, f, self.getDepAvailTime(instance, f, server)) + instance.filesDict[f].ctime < self.filesAvailTime[server][f]]
        while(len(can_fit) > 0):

            dep_name = can_fit[0]
            # if fname == 'c5t':
            #     rec_dep_time = self.getDepAvailTime(instance, dep_name, server)
            #     print(f'rec dep available at time {rec_dep_time}')
            #     print(f'reschedule {dep_name} at time {self.getEarliestGapToSched(instance, server, dep_name, rec_dep_time)} needed by {fname}')
            #     print(f'otherwise available at {self.filesAvailTime[server][dep_name]}')
            #     print(f'{dep_name} compilation time {instance.filesDict[dep_name].ctime}')
            #     print(f'already there {self.compSteps[server]}')
            #     print(f'currTime {self.currTime[server]}, depAvailTime {dep_avail_time}, dep cTime {instance.filesDict[dep_name].ctime}')
            # print(f'recompiling {dep_name} on server {server}')
            # print(f'otherwise available at {self.filesAvailTime[server][dep_name]}')
            self.add_step(dep_name, server, instance)
            # print(f'thought to be sched at {self.getEarliestGapToSched(instance, server, dep_name, self.getDepAvailTime(instance, dep_name, server))}')
            # print(f'scheduled at time {self.getSchedTime(dep_name, server)}')
            assert(dep_name in self.compSteps[server])
            
            # update relevant quantities
            dep_avail_time = self.getDepAvailTime(instance, fname, server)
            not_avail = [f for f in deps if self.filesAvailTime[server][f] > self.currTime[server]]
            can_fit = [f for f in not_avail if self.getEarliestGapToSched( \
                instance, server, f, self.getDepAvailTime(instance, f, server)) + instance.filesDict[f].ctime < self.filesAvailTime[server][dep_name]]
            # if fname == 'c5t':
            #     print(f'after rescheduling {dep_name} as needed by {fname}')
            #     print(f'available at {self.filesAvailTime[server][dep_name]}')
            #     print(f'steps now {self.compSteps[server]}')
            #     print(f'currTime {self.currTime[server]}, depAvailTime {dep_avail_time}')

        sched_time = self.getEarliestGapToSched(
            instance, server, fname, dep_avail_time)
        
        # if fname == 'c5t':
        #     print(f'c5t earliest gap {sched_time}')

        # make sure we do not schedule before all the dependencies are available
        sched_time = max(sched_time, dep_avail_time)

        # if fname == 'cu':
        #     print(f'cu sched time {sched_time}')

        # make sure we do not schedule twice a file on the same server
        if(fname not in self.compSteps[server]):
            # if fname == 'cu':
            #     print(f'record {fname} compilation at time {sched_time} and server {server}')
            self.recordNewCompilation(instance, sched_time, server, fname)

    def getEarliestServerForFile(self, fname: str, instance: SubInstance) -> int:
        """
        This function finds and returns the index of the earliest server we can compile file fname on.

        Args:
                fname (str): the name of the file to compile
                instance (SubInstance): the corresponding sub-problem to solve
        """

        assert(fname in instance.filesDict.keys())

        earliest_server = -1
        earliest_time = sys.maxsize
        for s in range(self.nservers):
            s_time = 0

            dep_avail_time = self.getDepAvailTime(instance, fname, s)
            s_time = self.getEarliestGapToSched(
                instance, s, fname, dep_avail_time)

            s_time = max(s_time, dep_avail_time)

            if (s_time < earliest_time):
                earliest_time = s_time
                earliest_server = s

        assert(earliest_server != -1)
        return earliest_server

    def getSchedTime(self, fname: str, server: int) -> int:
        assert((fname, server) in self.filesCompTimeDict.keys())
        time = self.filesCompTimeDict[(fname, server)]
        return time

    def printSolution(self, out_name: str):
        with open(out_name, 'w+') as f:
            print(len(self.filesCompTimeList), file=f)
            for sched_file in self.filesCompTimeList:
                print(f'{sched_file.fname} {sched_file.server}', file=f)
            f.close()

    def recordNewCompilation(self, instance: SubInstance, sched_time: int, server: int, fname: str):

        # if we creating a gap, record it
        if sched_time > self.currTime[server]:
            self.gaps[server] = True

        # update availability time
        if fname in self.filesAvailTime[server].keys():  # did we compile this file already?
            for otherS in range(instance.nservers):
                if otherS != server:
                    self.filesAvailTime[otherS][fname] = min(sched_time + instance.filesDict[fname].ctime + \
                        instance.filesDict[fname].rtime, self.filesAvailTime[otherS][fname])
                else:
                    self.filesAvailTime[otherS][fname] = min(sched_time + \
                        instance.filesDict[fname].ctime, self.filesAvailTime[otherS][fname])
        else:
            for otherS in range(instance.nservers):
                if otherS != server:
                    self.filesAvailTime[otherS][fname] = sched_time + instance.filesDict[fname].ctime + \
                        instance.filesDict[fname].rtime
                else:
                    self.filesAvailTime[otherS][fname] = sched_time + \
                        instance.filesDict[fname].ctime

        for s in range(instance.nservers):
            assert(fname in self.filesAvailTime[s].keys())

        # update time counter
        self.currTime[server] = max(
            sched_time + instance.filesDict[fname].ctime, self.currTime[server])
        # update dict (does not need to be sorted)
        self.filesCompTimeDict[(fname, server)] = sched_time

        # use insertion sort to update data structures which are kept orderd w.r.t. compilation time
        idx = 0
        file_before = None
        while(idx < len(self.filesCompTimeList)):
            if self.filesCompTimeList[idx].sched_time < sched_time:
                if self.filesCompTimeList[idx].server == server:
                    file_before = self.filesCompTimeList[idx].fname
                idx = idx + 1
            else:
                break
        self.filesCompTimeList.insert(
            idx, SchedFile(fname, sched_time, server))

        comp_steps_idx = 0
        if file_before is not None:
            comp_steps_idx = self.compSteps[server].index(
                file_before) + 1  # schedule just after
        self.compSteps[server].insert(comp_steps_idx, fname)

    def getDepAvailTime(self, instance: SubInstance, fname: str, server: int):

        all_dep_avail_time = 0
        # make sure the dependencies are available
        for dep in instance.filesDict[fname].dependencies:
            assert(dep in self.filesAvailTime[server])
            all_dep_avail_time = max(
                all_dep_avail_time, self.filesAvailTime[server][dep])

        return all_dep_avail_time

    def getEarliestGapToSched(self, instance: SubInstance, server: int, fname: str, dep_avail_time: int):

        # if there are gaps in the current schedule, try to fit the compilation there
        sched_time = 0
        if self.gaps[server]:
            # if fname == 'c5t':
            #     print(f'c5t steps {self.compSteps[server]}')
            for step in self.compSteps[server]:
                if self.getSchedTime(step, server) > sched_time + instance.filesDict[fname].ctime and \
                        sched_time >= dep_avail_time:
                    # can schedule here
                    # if fname == 'c5t':
                    #     print(f'can schedule before {step}, which was sched at {self.getSchedTime(step, server)}')
                    break
                else:
                    # if fname == 'c5t':
                    #     print(f'availtime of {step} is {self.filesAvailTime[server][step]}')
                    sched_time = self.filesAvailTime[server][step]
        else:
            sched_time = max(self.currTime[server], dep_avail_time)
            

        return sched_time


def loadSolution(fname: str, instance: Instance) -> Solution:
    with open(fname) as fp:
        # read metadata
        nsteps = int(fp.readline())
        sol = Solution(instance.nservers)
        for s in range(nsteps):
            name, server = fp.readline().split()
            server = int(server)
            assert(name in instance.files)
            assert(server >= 0 and server < instance.nservers)
            sol.compSteps[server].append(name)
        return sol
