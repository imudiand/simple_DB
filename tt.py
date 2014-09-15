import cmd


class CLI(cmd.Cmd):

	def do_SET(self, args):
		args = args.split()
		if len(args) == 2:

			if hasattr(self.db, 'queue') and hasattr(self.db, 'num_transactions') and self.db.num_transactions != 0:
				self.db.queue[self.db.num_transactions]['cmds'].append(self.db.set(key=args[0], val=args[1]))
				self.db.queue[self.db.num_transactions]['cache'][args[0]] = args[1]
			else:
				try:
					self.db.set(key=args[0], val=args[1])()
				except Exception as err:
					print "SET FAILED", err
		else:
			print "SET takes exactly 2 arguments"

	def do_GET(self, args):
		args = args.split()
		if len(args) == 1:
			if hasattr(self.db, 'queue') and hasattr(self.db, 'num_transactions') and self.db.num_transactions != 0:
				for i in xrange(self.db.num_transactions, 0, -1):
					if args[0] in self.db.queue[i]['cache']:
						print self.db.queue[i]['cache'][args[0]]
						return
			try:
				print self.db.get(key=args[0])
			except Exception:
				print 'GET FAILED'
		else:
			print "GET takes exactly 1 argument"


	def do_UNSET(self, args):
		args = args.split()
		if len(args) == 1:
			if hasattr(self.db, 'queue') and hasattr(self.db, 'num_transactions') and self.db.num_transactions != 0:
				self.db.queue[self.db.num_transactions]['cmds'].append(self.db.unset(key=args[0]))
				self.db.queue[self.db.num_transactions]['cache'][args[0]] = 'NULL'
			else:
				try:
					self.db.unset(key=args[0])()
				except Exception:
					print 'UNSET FAILED'
		else:
			print "UNSET takes exactly 1 argument"


	def do_NUMEQUALTO(self, args):
		args = args.split()
		if len(args) == 1:
			incache_count = 0
			if hasattr(self.db, 'queue') and hasattr(self.db, 'num_transactions') and self.db.num_transactions != 0:
				for i in xrange(self.db.num_transactions, 0, -1):
					if len(self.db.queue[i]['cache'].keys()) == 0:
						continue
					for k,v in self.db.queue[i]['cache'].iteritems():
						if v == args[0]:
							incache_count += 1
			try:
				print (incache_count + self.db.numequalto(val=args[0]))
			except Exception as err:
				print 'NUMEQUALTO FAILED', err
		else:
			print "NUMEQUALTO takes exactly 1 argument"

	def do_BEGIN(self, args):
		if hasattr(self.db, 'queue') and hasattr(self.db, 'num_transactions') and self.db.num_transactions != 0:
			self.db.num_transactions += 1
			self.db.queue[self.db.num_transactions] = dict()
			self.db.queue[self.db.num_transactions]['cmds'] = list()
			self.db.queue[self.db.num_transactions]['cache'] = dict()
		else:
			self.db.num_transactions = 1
			self.db.queue = dict()
			self.db.queue[self.db.num_transactions] = dict()
			self.db.queue[self.db.num_transactions]['cmds'] = list()
			self.db.queue[self.db.num_transactions]['cache'] = dict()

	def do_ROLLBACK(self, args):
		try:
			del self.db.queue[self.db.num_transactions]
			self.db.num_transactions -= 1
		except (AttributeError, KeyError) as err:
			print "NO TRANSACTION"


	def do_COMMIT(self, args):
		for k, v in self.db.queue.iteritems():
			for foo in v['cmds']:
				foo()
		self.db.queue.clear()
		self.db.num_transactions = 0


	def do_END(self, args):
		return True


class Transaction(object):
	num_of_transactions = 0
	def __init__(self):
		self.cmds = list()
		self.cache = dict()

	@staticmethod
	def inc_transaction():
		num_of_transactions += 1

	@staticmethod
	def dec_transaction():
		num_of_transactions -= 1

	def set(k, v):
		self.cmds.append(self.db.set(key=k, val=v))
		self.cache[k] = v

	def unset(k):
		self.cmds.append(self.db.unset(key=k))
		self.cache[k] = 'NULL'

	def get(k):
		if k in self.cache:
			return self.cache[k]

	def numequalto(val):
		incache_count = 0
		if len(self.cache.keys()) == 0:
			continue
		for k,v in self.cache.iteritems():
			if v == val:
				incache_count += 1
		return incache_count


class DB(object):
	
	def __init__(self):
		self.db = {}
		self.cli = CLI()
		self.cli.db = self
		self.cli.cmdloop("Harshit Imudianda - In Memory DB assignment")
		self.num_transactions = 0

	def get(self, key):
		try:
			return self.db[key]
		except KeyError:
			return 'NULL'

	def numequalto(self, val):
		count = 0
		for k,v in self.db.iteritems():
			if v == val:
				count += 1
		return count

	def set(self, key, val):
		def apply_set():
			try:
				self.db[key] = val
			except:
				raise
		return apply_set

	def unset(self, key):
		def apply_unset():
			try:
				del self.db[key]
			except KeyError:
				pass
		return apply_unset



def run():
	db = DB()

if __name__ == '__main__':
	run()