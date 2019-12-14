# -*- coding: utf-8 -*-

import argparse
import json
import logging
import sys
import time
from enum import IntEnum
from threading import Thread

import requests
from autobahn.twisted.util import sleep
from autobahn.twisted.websocket import connectWS, WebSocketClientFactory, WebSocketClientProtocol
from autobahn.websocket.compress import PerMessageDeflateOffer, PerMessageDeflateResponse, \
    PerMessageDeflateResponseAccept
from twisted.internet import reactor, ssl
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.protocol import ReconnectingClientFactory

logger = logging.getLogger('Scrapper')
language = 'en'  # en or cn
sport_type = 0  # 0 -> Football, 1 -> Basketball
timeout = 10


class Sport(IntEnum):
    FOOTBALL = 0
    BASKETBALL = 1


def to_json(string):
    arr = None
    dict_ = dict()
    try:
        data = string[:-1].split(';')
        for item in data:
            arr = item.split('=')
            dict_[arr[0]] = arr[1]
    except IndexError as e:
        logger.debug('[DEBUG] Line: {}. Exception: {}. Length of the array: {}'.format(
            sys.exc_info()[2].tb_lineno, e, len(arr)))
    return dict_


class MyClientProtocol(WebSocketClientProtocol):

    def __init__(self):
        self.loop_times = 1
        self.occurred_event_ids = list()
        super().__init__()

    def get_occurred_event_ids(self):
        return self.occurred_event_ids

    def add_occurred_event_id(self, event):
        self.occurred_event_ids.append(event)

    def data_parse(self, string):
        in_play_data = string.split('|CL;')
        football_data, basketball_data = str(), str()
        if len(in_play_data) >= 2:
            # soccer_data = in_play_data[1]
            for data in in_play_data:
                if 'ID=1;' in data and 'CD=1;' in data:
                    football_data = data
                elif 'ID=18;' in data:
                    basketball_data = data
        else:
            return  # End generator
        sport_data = football_data if Sport(sport_type) == Sport.FOOTBALL else basketball_data

        competitions = sport_data.split('|CT;')
        competitions = competitions[1:] if len(competitions) > 0 else list()
        for comp in competitions:
            data = comp.split('|EV;')
            league = to_json(data[0]).get('NA')
            for item in data[1:]:
                MA = to_json(item.split('|MA;')[0])
                event_id = MA['ID'][:8]
                score = MA['SS']
                # print(item.split('|MA;')[0])
                PA0 = item.split('|PA;')[0]
                PA0_json = to_json(PA0)
                TU, TT, TS, TM = PA0_json['TU'], int(PA0_json['TT']), int(PA0_json['TS']), int(PA0_json['TM'])
                begin_ts = time.mktime(time.strptime(TU, '%Y%m%d%H%M%S'))
                now_ts = time.time()
                # The match has not started. TT=0 means the match has not started or paused, TM=45 means in the midfield.
                retimeset = str(time.strftime('%H:%M:%S', time.localtime(now_ts - begin_ts))) if TT == 1 else '45:00'
                if TM == 0 and TT == 0:
                    retimeset = '00:00'
                details = item.split('|PA;')[1:]
                if len(details) >= 2:
                    home_team, away_team = to_json(details[0]).get('NA'), to_json(details[2]).get('NA')
                else:
                    home_team, away_team = str(), str()
                yield league, home_team, away_team, score, retimeset, event_id

        req = u'\x16\x00CONFIG_1_3,OVInPlay_1_3,Media_L1_Z3,XL_L1_Z3_C1_W3\x01'.encode('utf-8')  # English
        if language == 'cn':  # Chinese
            req = u'\x16\x00CONFIG_10_0,OVInPlay_10_0,Media_L10_Z0,XL_L10_Z0_C1_W3\x01'.encode('utf-8')
        logger.debug('[DEBUG] Waiting {} seconds...'.format(timeout))
        time.sleep(timeout)
        self.sendMessage(req)

    @inlineCallbacks
    def search(self, league, home_team, away_team, score, retimeset, event_id):
        yield sleep(0.3)
        self.occurred_event_ids.append(event_id)
        checklist = dict()
        checklist['data'] = {
            'id': event_id,
            'loop': self.loop_times,
            'league': league,
            'home_team': home_team,
            'away_team': away_team,
            'score': score,
            'elapsed_time': retimeset
        }
        print(json.dumps(checklist))
        req = u'\x16\x006V{}C18A_1_1\x01'.format(event_id).encode('utf-8')
        returnValue(req)

    @inlineCallbacks
    def subscribe_games(self, msg):
        increment = False
        for league, home_team, away_team, score, retimeset, event_id in self.data_parse(msg):
            try:
                req = yield self.search(league, home_team, away_team, score, retimeset, event_id)
                increment = True
            except Exception as e:
                logger.critical('[CRITICAL] Line: {}. Exception: {}. Closing connection...'.format(
                    sys.exc_info()[2].tb_lineno, e))
                self.sendClose(1000)
                sys.exit(1)
            else:
                self.sendMessage(req)
        if increment:
            self.loop_times += 1

    def sendMessage(self, command, *parameter_list, **kw):
        logger.debug("[DEBUG] Sending message: {}".format(command.decode('utf-8')))
        super().sendMessage(command, *parameter_list, **kw)

    def onOpen(self):
        req = str('\x23\x03P\x01__time,S_{}\x00'.format(
            self.factory.session_id)).encode('utf-8')
        self.sendMessage(req)

    @inlineCallbacks
    def onMessage(self, payload, is_binary):
        msg = payload.decode('utf-8')
        if msg.startswith('100'):
            req = u'\x16\x00CONFIG_1_3,OVInPlay_1_3,Media_L1_Z3,XL_L1_Z3_C1_W3\x01'.encode('utf-8')  # English
            if language == 'cn':  # Chinese
                req = u'\x16\x00CONFIG_10_0,OVInPlay_10_0,Media_L10_Z0,XL_L10_Z0_C1_W3\x01'.encode('utf-8')
            self.sendMessage(req)

        msg_header = 'OVInPlay_1_3'  # English
        if language == 'cn':  # Chinese
            msg_header = 'OVInPlay_10_0'

        if msg_header in msg:
            yield self.subscribe_games(msg)
        else:
            pass


class MyFactory(WebSocketClientFactory, ReconnectingClientFactory):

    def clientConnectionFailed(self, connector, reason):
        self.retry(connector)

    def clientConnectionLost(self, connector, reason):
        self.retry(connector)


def get_session_id(header_):
    url_ = 'https://www.bet365.com/defaultapi/sports-configuration'
    response = requests.get(url=url_, headers=header_)
    session_id = response.cookies['pstk']
    return session_id


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bet365 scrapper.')
    parser.add_argument('--sport', '-s', type=int, default=0, help='Sport type: 0 -> Football or 1 -> basketball')
    parser.add_argument('--timeout', '-t', type=int, default=1, help='Wait time between loops')
    parser.add_argument('--debug', '-d', action='store_true', help='Enable debug mode')
    args = parser.parse_args()
    timeout = args.timeout if args.timeout > 0 else 1
    if args.debug:
        logging.basicConfig(filename='scrapper.log', level=logging.DEBUG, format='%(asctime)s %(message)s')
    else:
        logging.basicConfig(filename='scrapper.log', level=logging.INFO, format='%(asctime)s %(message)s')
    if args.sport not in list(map(int, Sport)):
        print('[ERROR] Invalid sport selected: {}. Valid ones are 0 for football or 1 for basketball'.format(args.type))
        sys.exit(1)

    USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 " \
                 "(KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36"
    header_ = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0'}
    url = 'wss://premws-pt3.365lpodds.com/zap/'

    factory = WebSocketClientFactory(
        url, useragent=USER_AGENT, protocols=['zap-protocol-v1'])
    factory.protocol = MyClientProtocol
    factory.headers = header_

    factory.session_id = get_session_id(header_)


    def accept(response):
        if isinstance(response, PerMessageDeflateResponse):
            return PerMessageDeflateResponseAccept(response)


    factory.setProtocolOptions(perMessageCompressionAccept=accept)
    factory.setProtocolOptions(perMessageCompressionOffers=[PerMessageDeflateOffer(
        accept_max_window_bits=True,
        accept_no_context_takeover=True,
        request_max_window_bits=0,
        request_no_context_takeover=True,
    )])

    if factory.isSecure:
        contextFactory = ssl.ClientContextFactory()
    else:
        contextFactory = None
    connectWS(factory, contextFactory)
    Thread(target=reactor.run, args=(False,)).start()
