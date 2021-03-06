
import os
import sys
import pkgutil
import logging
from rq import Queue
from worker import conn
from apscheduler.scheduler import Scheduler

logging.basicConfig()

sched = Scheduler()

q = Queue(connection=conn)

# I'm not sure if this is even remotely necessary - or if sys.modules and __import__ cache things perfectly 
# intelligently - but here's a little hand-rolled cache of just the methods I want to call.
JOBS = {}

def setup_jobs():
	""" Does the initial gathering of the jobs. """
	for mod_lo, name, ispkg in pkgutil.iter_modules(["./jobs"]):
		if name not in JOBS:
			fullname = "jobs." + name
			x = __import__( fullname )
			this_job = getattr( x, name )
			if 'run' in dir( this_job ):
				JOBS[ name ] = getattr( this_job, 'run' )
				print "Added %s to the JOBS cache" % name


@sched.interval_schedule(minutes=5)
def timed_job():
	""" Runs the jobs defined by code in the jobs directory. """
	for name, job in JOBS.iteritems():
		print "Adding %s to the jobs queue" % name
		q.enqueue( job )


setup_jobs()
sched.start()

while True:
	pass	

