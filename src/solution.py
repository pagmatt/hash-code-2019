from collections import deque
from instance import *
import sys

class Solution():
	def __init__(self, nservers):
		self.nservers = nservers
		self.steps = [[] for s in range(self.nservers)]
		self.files = [{} for s in range(self.nservers)]
		self.time = [0 for s in range(self.nservers)]	# current time at each server
		self.files = [{} for s in range(self.nservers)] # files ready at each server (with corresponding time)

	def log(self):
		for s in range(self.nservers):
			print(self.steps[s])

	def evalCheck(self, instance):
		# queue of compilation steps for each server
		queues = [deque(self.steps[s]) for s in range(instance.nservers)]
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
				startTime = self.time[s]
				for dep in cf.dependencies:
					if dep not in self.files[s]:
						depOk = False
						break
					else:
						startTime = max(startTime, self.files[s][dep])
				if depOk:
					#print(f'Execute {cf.name} on server {s} at time {startTime}-{startTime+cf.ctime}')
					# remove it from queue
					queues[s].popleft()
					# we can execute f on s
					nDone += 1
					# mark f available (either compilation or replication)
					for otherS in range(instance.nservers):
						if otherS != s:
							self.files[otherS][cf.name] = startTime + cf.ctime + cf.rtime
						else:
							self.files[otherS][cf.name] = startTime + cf.ctime
					self.time[s] = startTime + cf.ctime
			if not nDone:
				break
		# if queues are not empty, then we have messed up with dependencies
		for s in range(instance.nservers):
			assert(not queues[s])
		# merge files from all servers
		targets = {}
		for s in range(instance.nservers):
			for f in self.files[s]:
				if f in targets:
					targets[f] = min(targets[f], self.files[s][f])
				else:
					targets[f] = self.files[s][f]
		# evaluate targets
		score = 0
		for t in targets:
			cf = instance.files[t]
			if (cf.points > 0) and (targets[t] <= cf.deadline):
				score += (cf.deadline - targets[t]) + cf.points

		# remove side-effects
		self.time = [0 for s in range(self.nservers)]
		self.files = [{} for s in range(self.nservers)]

		return score
	
	def add_step(self, fname: str, server: int, instance: SubInstance):
		assert(fname in instance.files[fname])
		max_aval_time = 0			
		for dep in instance.files[fname]:			# make sure the dependencies are available
			assert(dep in self.files[server])
			max_aval_time = max(max_aval_time, self.files[server][dep])

		# schedule this compilation
		sched_time = max(max_aval_time, self.time[server])
		for otherS in range(instance.nservers):
			if otherS != server:
				self.files[otherS][fname] = sched_time + instance.files[fname].ctime + \
												instance.files[fname].rtime
			else:
				self.files[otherS][fname] = sched_time + instance.files[fname].ctime
		self.time[server] = sched_time + instance.files[fname].ctime
	
	def get_earliest_server_for_file(self, fname: str, instance: SubInstance):
		assert(fname in instance.files[fname])

		# should have scheduled all dependencies already
		earliest_server = -1
		earliest_time = sys.maxsize
		for s in range(self.nservers):
			s_time = 0
			for dep in instance.files[fname]:
				assert(dep in self.files[s])
				s_time = max(earliest_time, self.files[s][dep])
			
			if (s_time < earliest_time):
				earliest_time = s_time
				earliest_server = s

		assert(earliest_server !=  -1)
		return earliest_server

def loadSolution(fname: str, instance: Instance):
	with open(fname) as fp:
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
