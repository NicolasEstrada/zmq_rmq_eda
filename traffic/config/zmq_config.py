
sensor = dict(
	incoming = dict(
		host = 'localhost',
		port = 10000,
		socket_type = 'SUB'
		),

	outgoing = dict(
		host = 'localhost',
		port = 11000,
		socket_type = 'PUB'
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
		routing_key = 'CEP'
		),

	outgoing = dict(
		host = 'localhost',
		port = 13000,
		socket_type = 'PUSH'
		)
	)
