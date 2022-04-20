import numpy as np
from matplotlib import pyplot as plt
class CompiledFile():
	def __init__(self, name, ctime, rtime, dependencies):
		self.name = name
		self.ctime = ctime
		self.rtime = rtime
		self.dependencies = dependencies
		self.deadline = -1
		self.points = 0
	def __str__(self):
		return f'{self.name} ctime={self.ctime}, rtime={self.rtime}, deps={self.dependencies}, deadline={self.deadline}, points={self.points}'

class Instance():
	def __init__(self, files, targets, nservers):
		self.files = files
		self.targets = targets
		self.nservers = nservers
	def log(self):
		for f in self.files:
			print(self.files[f])
		print(self.targets)
		print(self.nservers)

	def plot_points_distribution(self):
		points = [self.files[t_name].points for t_name in self.targets]
		plt.hist (points)
		plt.show ()

	def plot_deadlines_distribution(self):
		points = [self.files[t_name].deadline for t_name in self.targets]
		plt.hist (points)
		plt.show ()


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
			name, ctime, rtime = fp.readline().split()
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
			name, deadline, points = fp.readline().split()
			assert(name in files)
			files[name].deadline = int(deadline)
			files[name].points = int(points)
			targets.append(name)
		return Instance(files, targets, nservers)

class SubInstance ():
	def __init__(self, filesList: list[CompiledFile], filesDict: dict, target: list, nservers):
		self.filesList = filesList
		self.filesDict = filesDict
		self.target = target
		self.nservers = nservers

	def log(self):
		print('Files:')
		for f in self.filesList:
			print(f)
		print('Target:')
		print(self.target)
		print('# servers:')
		print(self.nservers)

	def get_times_and_idx(self, fname):
		for idx in range(len(self.filesList)):
			if self.filesList[idx].name == fname:
				return [self.filesList[idx].ctime, self.filesList[idx].rtime, idx]

	def get_deadline(self):
		assert(self.target in self.filesDict.keys())
		return self.filesDict[self.target].deadline

	def get_compil_points(self):
		assert(self.target in self.filesDict.keys())
		return self.filesDict[self.target].points
