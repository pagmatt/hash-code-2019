#!/usr/bin/env python

from collections import deque


class CompiledFile(object):
	def __init__(self, name, ctime, rtime, dependencies):
		super(CompiledFile, self).__init__()
		self.name = name
		self.ctime = ctime
		self.rtime = rtime
		self.dependencies = dependencies
		self.deadline = -1
		self.points = 0
	def __str__(self):
		return f'{self.name} ctime={self.ctime}, rtime={self.rtime} deps={self.dependencies} deadline={self.deadline} points={self.points}'


class Instance(object):
	def __init__(self, files, targets, nservers):
		super(Instance, self).__init__()
		self.files = files
		self.targets = targets
		self.nservers = nservers
	def log(self):
		for f in self.files:
			print(self.files[f])
		print(self.targets)
		print(self.nservers)


def loadInstance(filename):
	with open(filename) as fp:
		# read metadata
		nfiles, ntargets, nservers = [int(x) for x in fp.readline().split()]
		assert(nfiles >= 1 and nfiles <= 100000)
		assert(ntargets >= 1 and ntargets <= nfiles)
		assert(nservers >= 1 and nservers <= 100)
		# read compiled files
		files = {}
		for c in range(nfiles):
			name,ctime,rtime = fp.readline().split()
			ctime = int(ctime)
			rtime = int(rtime)
			tokens = fp.readline().split()
			ndeps = int(tokens[0])
			deps = tokens[1:]
			assert(len(deps) == ndeps)
			files[name] = CompiledFile(name, ctime, rtime, deps)
		# read targets
		targets = []
		for t in range(ntargets):
			name,deadline,points = fp.readline().split()
			assert(name in files)
			files[name].deadline = int(deadline)
			files[name].points = int(points)
			targets.append(name)
		return Instance(files,targets,nservers)


class Solution(object):
	def __init__(self, nservers):
		super(Solution, self).__init__()
		self.nservers = nservers
		self.steps = [[] for s in range(self.nservers)]
		self.files = [{} for s in range(self.nservers)]
	def log(self):
		for s in range(self.nservers):
			print(self.steps[s])


def loadSolution(filename, instance):
	with open(filename) as fp:
		# read metadata
		nsteps = int(fp.readline())
		sol = Solution(instance.nservers)
		for s in range(nsteps):
			name, server = fp.readline().split()
			server = int(server)
			assert(name in instance.files)
			assert(server >= 0 and server < instance.nservers)
			sol.steps[server].append(name)
		return sol


def evalCheck(instance, solution):
	# queue of compilation steps for each server
	queues = [deque(solution.steps[s]) for s in range(instance.nservers)]
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
				# mark f available (either compilation or replication)
				for otherS in range(instance.nservers):
					if otherS != s:
						files[otherS][cf.name] = startTime + cf.ctime + cf.rtime
					else:
						files[otherS][cf.name] = startTime + cf.ctime
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


if __name__ == '__main__':
	import sys
	instance = loadInstance(sys.argv[1])
	#instance.log()
	solution = loadSolution(sys.argv[2], instance)
	#solution.log()
	print("Score =", evalCheck(instance, solution))
