from celery import Celery

app = Celery('tasks', backend='amqp', broker='amqp://guest@localhost//')

@app.task
def add(x, y):
    return x + y


@app.task
def tsum(numbers):
    return sum(numbers)

