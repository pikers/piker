#!/usr/bin/env python
import time
import json
from datetime import datetime, timedelta
import requests

from cryptofeed import FeedHandler
from cryptofeed.callback import OrderInfoCallback, BalancesCallback, UserFillsCallback
from cryptofeed.exchanges.deribit import Deribit
from cryptofeed.defines import DERIBIT, TRADES, OPEN_INTEREST, OPTION, CALL, PUT
from cryptofeed.symbols import Symbol

from elasticsearch import Elasticsearch

from .elastic import (
	ES_HOST,
	oi_mapping,
	trades_mapping,
	es_prefix
)

def start_max_pain_daemon():

	config = \
	{
		'log':{
			'filename': 'feedhandler.log',
			'level': 'INFO'
		},
		'deribit': {
			'key_id': 'KPJN6bFQ',
			'key_secret': 'TMPatQRXMQUQ83OCVoqYomcMvPYpTUPOUqayuJJ3zMA'
		}
	}

	# maybe import this function from ṕiker.brokers.deribit.api
	def piker_sym_to_cb_sym(name: str) -> Symbol:
		base, expiry_date, strike_price, option_type = tuple(
			name.upper().split('-'))
		quote = base

		if option_type == 'P':
			option_type = PUT
		elif option_type  == 'C':
			option_type = CALL
		else:
			raise Exception("Couldn\'t parse option type")

		return Symbol(
			base,
			quote,
			type=OPTION,
			strike_price=strike_price,
			option_type=option_type,
			expiry_date=expiry_date.upper()
		)

	def get_instruments(currency, kind):
		payload = {'currency': currency, 'kind': kind}
		r = requests.get('https://test.deribit.com/api/v2/public/get_instruments', params=payload)
		resp = json.loads(r.text)
		response_list = []

		# For now only get half of the instruments
		for i in range(len(resp['result']) // 2):
			element = resp['result'][i]
			response_list.append(piker_sym_to_cb_sym(element['instrument_name']))

		return response_list

	# trade and oi are user defined functions that
	# will be called when trade and open interest updates are received
	# data type is not dict, is an object: cryptofeed.types.OpenINterest
	async def oi(data: dict, receipt_timestamp):

		# Get timestamp and convert it to isoformat
		date = (datetime.utcfromtimestamp(data.timestamp)).isoformat()

		index = es_prefix(data.symbol, 'oi')
		document = {
			'timestamp': date,
			'open_interest': data.open_interest
		}
		#Save to db
		es.index(index=index, document=document)
		print('Saving to db...')
		print(date)
		print(data)

	# Data type is not dict, is an object: cryptofeed.types.Ticker
	async def trade(data: dict, receipt_timestamp):
		# Get timestamp and convert it to isoformat
		date = (datetime.utcfromtimestamp(data.timestamp)).isoformat()

		index = es_prefix(data.symbol, 'trades')
		document = {
			'direction': data.side,
			'amount': data.amount,
			'price': data.price,
			'timestamp': date,
		}
		#Save to db
		es.index(index=index, document=document)
		print('Saving to db...')
		print(date)
		print(data)

	callbacks = {TRADES: trade, OPEN_INTEREST: oi}

	fh = FeedHandler(config=config)

	fh.add_feed(
		DERIBIT,
		channels=[TRADES, OPEN_INTEREST],
		symbols=get_instruments('BTC', 'option'),
		callbacks=callbacks
	)

	es = Elasticsearch(ES_HOST)

	es.indices.put_template(
		name='oi_mapping',
		body=oi_mapping)

	es.indices.put_template(
		name='trades_mapping',
		body=trades_mapping)

	fh.run()

# if __name__ == '__main__':
# 	main()
