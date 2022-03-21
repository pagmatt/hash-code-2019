class CompiledFile():
	def __init__(self, name, ctime, rtime, dependencies):
		self.name = name
		self.ctime = ctime
		self.rtime = rtime
		self.dependencies = dependencies
		self.deadline = -1
		self.points = 0
	def __str__(self):
		return f'{self.name} ctime={self.ctime}, rtime={self.rtime} deps={self.dependencies} deadline={self.deadline} points={self.points}'


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