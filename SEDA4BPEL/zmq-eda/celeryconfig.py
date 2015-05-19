BROKER_URL = 'amqp://'
CELERY_RESULT_BACKEND = 'amqp://'

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT=['json']
CELERY_TIMEZONE = 'Europe/Oslo'
CELERY_ENABLE_UTC = True

# CELERY_ROUTES = {
#     'tasks.add': 'low-priority',
# }

# CELERY_ANNOTATIONS = {
#     'tasks.add': {'rate_limit': '10/m'}
# }

CELERY_RESULT_PERSISTENT = True

# (1) --- Automatic routing ---

# CELERY_ROUTES = {'feed.tasks.import_feed': {'queue': 'feeds'}}
# $ celery -A proj worker -Q feeds
# $ celery -A proj worker -Q feeds, celery
# celery is the default queue

# (2) --- Changing default names ---
from kombu import Exchange, Queue

CELERY_DEFAULT_QUEUE = 'zmq-eda'
CELERY_QUEUES = (
    Queue('zmq-eda', Exchange('zmq-eda'), routing_key='zmq-eda'),
)

# (3) --- Manual Routing ---

# CELERY_DEFAULT_QUEUE = 'default'
# CELERY_QUEUES = (
#     Queue('default',    routing_key='task.#'),
#     Queue('feed_tasks', routing_key='feed.#'),
# )
# CELERY_DEFAULT_EXCHANGE = 'tasks'
# CELERY_DEFAULT_EXCHANGE_TYPE = 'topic'
# CELERY_DEFAULT_ROUTING_KEY = 'task.default'

# CELERY_ROUTES = {
#         'feeds.tasks.import_feed': {
#             'queue': 'feed_tasks',
#             'routing_key': 'feed.import',
#         },
# }

# (4) --- Override routing ---

# from feeds.tasks import import_feed
# import_feed.apply_async(args=['http://cnn.com/rss'],
#                         queue='feed_tasks',
#                         routing_key='feed.import')


# (5) --- Consuming tasks from multiple exchanges types

# from kombu import Exchange, Queue

# CELERY_QUEUES = (
#     Queue('feed_tasks',    routing_key='feed.#'),
#     Queue('regular_tasks', routing_key='task.#'),
#     Queue('image_tasks',   exchange=Exchange('mediatasks', type='direct'),
#                            routing_key='image.compress'),
# )

# --- DIRECT WORKER

# CELERY_WORKER_DIRECT = True
# CELERY_ROUTES = {
#     'tasks.add': {'exchange': 'C.dq', 'routing_key': 'w1@example.com'}
# }


