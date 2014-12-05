from celery import chord
from tasks import add, tsum

print chord(add.s(i, i) for i in xrange(100))(tsum.s()).get()