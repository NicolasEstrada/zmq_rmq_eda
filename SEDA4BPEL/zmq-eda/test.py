from message_profiler import MessageProfiler

with MessageProfiler('test', True) as mp:
	for i in xrange(100):
		mp.msg_sent(100)

		mp.msg_received(100)

import pdb; pdb.set_trace()

print mp


print 1

print 2
