from collections import deque
import sched
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
		self.compSteps = [[] for s in range(self.nservers)]			# compilation steps performed at each server. Kept in chronological order
		self.filesAvailTime = [{} for s in range(self.nservers)]	# times when files are ready at each server 
		self.filesCompTimeList = []
		self.filesCompTimeDict = {}									# faster to search, but not sorted time wise
		self.currTime = [0 for s in range(self.nservers)]			# current time at each server ~ last instant during which a file is compiled
		self.gaps = [False for s in range(self.nservers)]			# are there gaps between compilation in a given server ?

	def log(self):
		for s in range(self.nservers):
			print(self.compSteps[s])

	def evalCheck(self, instance):
		# prevent side-effects
		avail_times = [{} for s in range(self.nservers)]
		times = [0 for s in range(self.nservers)]
		# queue of compilation steps for each server
		queues = [deque(self.compSteps[s]) for s in range(instance.nservers)]
		
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
				startTime = times[s]
				for dep in cf.dependencies:
					if dep not in avail_times[s]:
						depOk = False
						break
					else:
						startTime = max(startTime, avail_times[s][dep])
				if depOk:
					#print(f'Execute {cf.name} on server {s} at time {startTime}-{startTime+cf.ctime}')
					# remove it from queue
					queues[s].popleft()
					# we can execute f on s
					nDone += 1
					# mark f available (either compilation or replication)
					for otherS in range(instance.nservers):
						if otherS != s:
							avail_times[otherS][cf.name] = startTime + cf.ctime + cf.rtime
						else:
							avail_times[otherS][cf.name] = startTime + cf.ctime
					times[s] = startTime + cf.ctime
			if not nDone:
				break
		# if queues are not empty, then we have messed up with dependencies
		for s in range(instance.nservers):
			assert(not queues[s])
		# merge files from all servers
		targets = {}
		for s in range(instance.nservers):
			for f in avail_times[s]:
				if f in targets:
					targets[f] = min(targets[f], avail_times[s][f])
				else:
					targets[f] = avail_times[s][f]
		# evaluate targets
		score = 0
		for t in targets:
			cf = instance.files[t]
			if (cf.points > 0) and (targets[t] <= cf.deadline):
				score += (cf.deadline - targets[t]) + cf.points

		return score
	
	def add_step(self, fname: str, server: int, instance: SubInstance):
		assert(fname in instance.filesDict.keys())
		avail_time, s_time = 0, 0		
		for dep in instance.filesDict[fname].dependencies:			# make sure the dependencies are available
			assert(dep in self.filesAvailTime[server])
			avail_time = max(avail_time, self.filesAvailTime[server][dep])

		if self.gaps[server]:
			for step in self.compSteps[server]:
				if self.getSchedTime(step, server) > s_time + instance.filesDict[fname].ctime and \
					s_time >= avail_time:
					# can sched here
					break;
				else:
					s_time = self.filesAvailTime[server][step]
		else:
			s_time = self.currTime[server]
		
		s_time = max(s_time, avail_time)

		# are we creating a gap?
		if s_time > self.currTime[server]:
			self.gaps[server] = True

		# schedule dependencies twice (on a different server) if it makes sense to do so
		#while(avail_time > min(self.currTime)):
		
		# make sure we do not schedule twice a file on the same server
		if(fname not in self.compSteps[server]):	
			for otherS in range(instance.nservers):
				if otherS != server:
					self.filesAvailTime[otherS][fname] = s_time + instance.filesDict[fname].ctime + \
													instance.filesDict[fname].rtime
				else:
					self.filesAvailTime[otherS][fname] = s_time + instance.filesDict[fname].ctime		
			self.currTime[server] = max(s_time + instance.filesDict[fname].ctime, self.currTime[server])
			# self.occupancy[server] = self.occupancy[server] + instance.filesDict[fname].ctime
			idx = 0
			file_before = None
			while(idx < len(self.filesCompTimeList)):
				if self.filesCompTimeList[idx].sched_time < s_time:
					if self.filesCompTimeList[idx].server == server:
						file_before = self.filesCompTimeList[idx].fname
					idx = idx + 1
				else:
					break
			self.filesCompTimeList.insert(idx, SchedFile(fname, s_time, server))

			# re-order comp steps if compiled in a gap. TODO: use insertion sort here as well
			# idx = 0
			# while(idx < len(self.compSteps[server])):
			# 	if (self.compSteps[server].sched_time < s_time):
			# 		idx = idx + 1
			# 	else:
			# 		break
			# self.filesCompTime.insert(idx, SchedFile(fname, s_time, server))
			comp_steps_idx = 0
			if file_before is not None:
				comp_steps_idx = self.compSteps[server].index(file_before) + 1
			self.compSteps[server].insert(comp_steps_idx, fname) 
			self.filesCompTimeDict[(fname, server)] = s_time

	
	def get_earliest_server_for_file(self, fname: str, instance: SubInstance):
		assert(fname in instance.filesDict.keys())

		# should have scheduled all dependencies already
		earliest_server = -1
		earliest_time = sys.maxsize
		for s in range(self.nservers):
			s_time, avail_time = 0, 0

			for dep in instance.filesDict[fname].dependencies:
				assert(dep in self.filesAvailTime[s])
				avail_time = max(avail_time, self.filesAvailTime[s][dep])

			for step in self.compSteps[s]:
				if self.getSchedTime(step, s) > s_time + instance.filesDict[fname].ctime and \
					s_time >= avail_time:
					# can sched here
					break;
				else:
					s_time = self.filesAvailTime[s][step]

			s_time = max(s_time, avail_time)
				
			if (s_time < earliest_time):
				earliest_time = s_time
				earliest_server = s

		assert(earliest_server !=  -1)
		return earliest_server

	def getSchedTime(self, fname: str, server: int):
		assert((fname, server) in self.filesCompTimeDict.keys())
		time = self.filesCompTimeDict[(fname, server)]
		return time

	def printSolution(self, out_name: str):
		with open(out_name, 'w+') as f:
			print(len(self.filesCompTimeList), file=f)
			for sched_file in self.filesCompTimeList:
				print(f'{sched_file.fname} {sched_file.server}', file=f)

			f.close()

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
			sol.compSteps[server].append(name)
		return sol
