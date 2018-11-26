#
# Event-driven code that behaves as either a client or a server
# depending on the argument.  When acting as client, it connects
# to a server and periodically sends an update message to it.
# Each update is acked by the server.  When acting as server, it
# periodically accepts messages from connected clients.  Each
# message is followed by an acknowledgment.
#
# Tested with Python 2.7.8 and Twisted 14.0.2
#
import optparse

from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet import reactor
import random

peerList = []
lamportClock = 0
messages = []
peerCount = 0


def parse_args():
    usage = """usage: %prog [options] hostname pid port
    python peer.py 127.0.0.1 0 2487 """

    parser = optparse.OptionParser(usage)

    _, args = parser.parse_args()

    if len(args) != 3:
        print parser.format_help()
        parser.exit()

    return args


def shouldBeginSending():
    return peerCount == Const.NUMBER_OF_PEERS - 1


def isAck(message):
    return '<ACK' in message


def getRandomWaitingTime():
    return random.uniform(Const.MIN_DELAY, Const.MAX_DELAY)


def sortMessages():
    global messages
    messages = sorted(messages, key=lambda e: (e.timestamp, e.pid))


class Peer(Protocol):
    acks = 0
    connected = False
    messageCounter = 0
    lamportClocks = []
    pid = -1

    def __init__(self, factory, pid, fp):
        self.pid = pid
        self.fp = fp
        self.factory = factory
        self.initializeLamportClock()

    def connectionMade(self):
        global peerCount, peerList
        peerCount = peerCount + 1
        self.connected = True

        peerList.append(self)

        if shouldBeginSending():
            self.sendUpdate()

    def initializeLamportClock(self):
        for i in range(0, Const.NUMBER_OF_PEERS):
            self.lamportClocks.append(0.0)

    def advanceMyClock(self):
        self.lamportClocks[self.pid] += 1.0

    def sendUpdate(self):

        print "Sending Message"

        self.advanceMyClock()

        message = Message(messageType=Const.MESSAGE_TYPE_NORMAL, senderId=self.pid,
                          timestamp=self.lamportClocks[self.pid], counter=self.messageCounter)

        try:

            for i in range(0, peerCount):

                if peerList[i] != self.transport:
                    peerList[i].transport.write(message.toString())

            self.messageCounter += 1

        except Exception, ex1:

            print "Exception trying to send: ", ex1.args[0]

        if self.connected & self.messageCounter <= Const.MESSAGE_LIMIT:
            reactor.callLater(getRandomWaitingTime(), self.sendUpdate())

    def deliver(self):
        notDeliveredMessages = []

        for message in self.messages:
            if message.timestamp <= min(self.lamportClocks):
                self.writeMessage(message.toString())
            else:
                notDeliveredMessages.append(message)

        global messages
        del messages[:]
        messages = notDeliveredMessages[:]

    def writeMessage(self, message):
        self.fp.write(message + '\n')

    def sendAck(self):
        print 'Sending Ack'
        try:
            for peer in peerList:
                message = Message(messageType=Const.MESSAGE_TYPE_ACK, senderId=self.pid,
                                  timestamp=self.lamportClock[self.pid])
                peer.transport.write(message.toString())
        except Exception, e:
            print e.args[0]

    def dataReceived(self, data):
        message = Message(messageString=data)
        if message.type == Const.MESSAGE_TYPE_NORMAL:
            messages.append(message)
            self.advanceMyClock()
            self.sendAck()
        else:
            self.acks += 1
        self.lamportClocks[self.pid] = max(message.timestamp, self.lamportClock[self.pid]) + 1
        self.lamportClocks[message.senderId] = message.timestamp
        sortMessages()
        self.deliver()

    def connectionLost(self, reason):
        print "Disconnected"
        self.connected = False
        self.done()

    def done(self):
        self.factory.finished(self.acks)


class Message:
    counter = ""
    senderId = -1
    messageType = 0
    timestamp = 0.0

    def __init__(self, messageType=None, senderId=None, timestamp=None, counter=None, messageString=None):
        # type: (int, int, float, int, str) -> Message
        if messageString is None:
            self.type = messageType
            self.timestamp = timestamp
            self.senderId = senderId

            if messageType == Const.MESSAGE_TYPE_NORMAL:
                self.counter = counter
                messages.append(self)
        else:
            if isAck(messageString):
                _, self.senderId, self.timestamp, = messageString.split(':')
                self.messageType = Const.MESSAGE_TYPE_ACK
            else:
                _, self.counter, self.senderId, self.timestamp = messageString.split(':')
                self.messageType = Const.MESSAGE_TYPE_NORMAL

    def toString(self):
        if self.messageType == Const.MESSAGE_TYPE_NORMAL:
            return "<ACK>:" + str(self.senderId) + ":" + str(self.timestamp)
        else:
            return "<MESSAGE>:" + str(self.counter) + ":" + str(self.senderId) + ":" + str(self.timestamp)


class Const:
    def __init__(self):
        pass

    MESSAGE_TYPE_NORMAL = 1
    MESSAGE_TYPE_ACK = 2
    MESSAGE_LIMIT = 20
    NUMBER_OF_PEERS = 3
    MIN_DELAY = 2.0
    MAX_DELAY = 3.0


class PeerFactory(ClientFactory):
    def __init__(self, peertype, pid):
        print '@__init__'
        self.pt = peertype
        self.pid = pid
        self.acks = 0
        self.fname = "delivered-messages-" + str(pid)
        self.records = []

    def finished(self, arg):
        self.acks = arg
        self.report()

    def report(self):
        print 'Received %d acks' % self.acks

    def clientConnectionFailed(self, connector, reason):
        print 'Failed to connect to:', connector.getDestination()
        self.finished(0)

    def clientConnectionLost(self, connector, reason):
        print 'Lost connection.  Reason:', reason

    def startFactory(self):
        print "@startFactory"
        self.fp = open(self.fname, 'w+')

    def stopFactory(self):
        print "@stopFactory"
        self.fp.close()

    def buildProtocol(self, addr):
        print "@buildProtocol"
        protocol = Peer(self, self.pid, self.fp)
        return protocol


if __name__ == '__main__':
    host, pid, port = parse_args()

    if pid == '0':
        print "Start listening : Waiting for peers to connect @" + host + ":" + str(port)
        factory = PeerFactory('pid_0', int(pid))
        reactor.listenTCP(int(port), factory)
    elif pid == '1':
        print "Connecting to host peer 0 @" + host + ":" + str(port)
        factory = PeerFactory('pid_1', int(pid))
        reactor.connectTCP(host, int(port), factory)
        print "Start listening : Waiting for peer 2 to connect @" + host + ":" + str(port)
        factory = PeerFactory('pid_1', int(pid))
        reactor.listenTCP(int(port), factory)
    elif pid == '2':
        print "Connecting to host peer 0 @" + host + ":" + str(port)
        factory = PeerFactory('pid_2', int(pid))
        reactor.connectTCP(host, int(port), factory)
        print "Connecting to host peer 1 @" + host + ":" + str(port)
        factory = PeerFactory('pid_2', int(pid))
        reactor.connectTCP(host, int(port), factory)

    reactor.run()
