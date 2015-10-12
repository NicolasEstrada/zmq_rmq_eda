
generator = dict(
	outgoing = dict(
		host = '*',
		port = 10000,
		socket_type = 'XPUB'
		)
	)

sensor = dict(
	incoming = dict(
		host = 'localhost',
		port = 10000,
		socket_type = 'SUB'
		),

	outgoing = dict(
		host = 'localhost',
		port = 11000,
		socket_type = 'PUSH'
		)
	)

receiver = dict(
	incoming = dict(
		host = '*',
		port = 11000,
		socket_type = 'PULL'
		),

	outgoing = dict(
		host = '*',
		port = 12000,
		socket_type = 'XPUB'
		)
	)

controller = dict(
	incoming = dict(
		host = 'localhost',
		port = 12000,
		socket_type = 'SUB',
		routing_key = 'event'
		),

	outgoing = dict(
		host = 'localhost',
		port = 13000,
		socket_type = 'PUSH'
		),

	cep = dict(
		host = 'localhost',
		port = 14000,
		socket_type = 'PUSH'
		)
	)

data = dict(
	incoming = dict(
		host = '*',
		port = 13000,
		socket_type = 'PULL'
		),

	disk = dict(
		path = './data.csv'
		)
	)

cep = dict(
	incoming = dict(
		host = '*',
		port = 14000,
		socket_type = 'PULL'
		),

	outgoing = dict(
		host = 'localhost',
		port = 13000,
		socket_type = 'PUSH'
		),

	cep_agg_out = dict(
		host = '*',
		port = '15000',
		socket_type = 'XPUB'
		),

	events = dict (
		send_event = (-1, 1, 2, 3, 4, 5),
		cep_agg = (2, 3, 4)
		),

	db = dict(
		host = 'localhost',
		port = 6379,
		db_index = 0
		)
	)

aggregator = dict(
	incoming = dict(
		host = 'localhost',
		port = 15000,
		socket_type = 'SUB',
		routing_key = 'agg'
		),

	outgoing = dict(
		host = 'localhost',
		port = 13000,
		socket_type = 'PUSH'
		),

	events = dict (
		send_event = (10,)
		),
	)