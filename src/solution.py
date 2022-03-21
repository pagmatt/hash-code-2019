from collections import deque

class Solution():
	def __init__(self, nservers):
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