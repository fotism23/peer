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
import time

peerlist  = []
lamportClocks = []
messages = []
peercount = 0
MESSAGE_LIMIT = 20


# DONE - PARSE ARGUMENTS
def parse_args():
	usage = """usage: %prog [options] hostname pid port

	python peer.py 127.0.0.1 0 2487 """

	parser = optparse.OptionParser(usage)

	_, args = parser.parse_args()

	if len(args) != 3:
		print parser.format_help()
		parser.exit()

	host, pid, port = args

	return host, pid, port

class Peer(Protocol):
	
	acks = 0
	peerId = NIL
	# connected = False
	messageCounter = 0
	
	def __init__(self, factory, pid):
		self.pid = pid
		self.factory = factory

	def connectionMade(self):
		global peercount, peerlist
		peercount = peercount + 1

		print "Connected from ", self.transport.client
		peerlist.append(self)

		if self.pid == 0:
			self.initializeLamportClock()

		if self.shouldBeginSending():
			self.sendMessage()

		# if self.pt == 'client':
		# 	self.connected = True
		# else:
		# 	print "Connected from", self.transport.client
		# 	peerlist.append(self)
		# 	try:
		# 		self.transport.write('<connection up>')
		# 		reactor.callLater(5, self.sendUpdate)
		# 	except Exception, e:
		# 		print e.args[0]
		# 	self.ts = time.time()

	def sendMessage(self):
		print "Sending Message"
		
		# todo kick lamportClock
		# todo get my lamportClock

		try:
			for i in range(0, peercount):
				if peerlist[i] != self.transport:		
					peerlist[i].transport.write(message)
					# peerlist[i].transport.write('<update'+ " "+str(counti)+'>\n')
			messageCounter += 1
		except Exception, ex1:
			print "Exception trying to send: ", ex1.args[0]
		if self.connected == True:
			reactor.callLater(5, self.sendUpdate)

	def sendAck(self):
		print "sendAck"
		self.ts = time.time()
		try:
			self.transport.write('<Ack>')
		except Exception, e:
			print e.args[0]

	def dataReceived(self, data):
		if self.pt == 'client':
			print 'Client received ' + data
			self.acks += 1
			self.sendAck()

		else:
			print 'Server received ' + data

	# TODO CHECK FOR CONNECTED VARIABLE
	def connectionLost(self, reason):
		print "Disconnected"
		if self.pt == 'client':
			self.connected = False
			self.done()
	
	# DONE - CHECKS IF ONE OR MORE PEERS HAVE BENN CREATED
	def shouldBeginSending(self):
		return peercount > 1

	def done(self):
		self.factory.finished(self.acks)


# TODO IMPLEMENT THIS
class LamportClock:

	def __init__(self)
		pass


# DONE - MESSAGE (NORMAL OR ACK) NORMAL FORMAT MESSAGE:LC:SENDER_PID
#		 						 ACK FORMAT	   <ACK>:LC:SENDER_ID
class Message:
	MESSAGE_TYPE_NORMAL = 1
	MESSAGE_TYPE_ACK = 2
	
	content = ""
	timestamp = 0.0
	senderId = -1
	messageType = 0
	lamprotClock = None

	def __init__(self, senderId ,content, timestamp):
		self.senderId = senderId
		self.content = content
		self.timestamp = timestamp

	def __init__(self, string):
		if isAck(string):
			self.messageType = MESSAGE_TYPE_ACK
			_, self.lamportClock, self.senderId = string.split(':')
		else
			self.content, self.lamportClock, self.senderId = string.split(':')
			self.messageType = MESSAGE_TYPE_NORMAL

	def isAck(self):
		return '<ACK>' in self.content

	def isAck(message):
		return '<ACK>' in message

	
# DONE
class PeerFactory(ClientFactory):

	def __init__(self, peertype, pid):
		print '@__init__'
		self.pt = peertype
        self.pid = pid
		self.acks = 0
		self.fname = peertype
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
		if self.pt == 'server':
			self.fp = open(self.fname, 'w+')

	def stopFactory(self):
		print "@stopFactory"
		if self.pt == 'server':
			self.fp.close()

	def buildProtocol(self, addr):
		print "@buildProtocol"
		protocol = Peer(self, self.pt)
		return protocol


# TODO IMPLEMENT THIS
if __name__ == '__main__':
	host, pid, port = parse_args()

    if pid == '0':
        factory = PeerFactory('pid_0', pid)
        reactor.listenTCP(int(port), factory)
        print "Starting server @" + host + " port " + str(port)
    elif pid == '1':
        factory = PeerFactory('pid_1', pid)
        print "Connecting to host " + host + " port " + str(port)
        reactor.connectTCP(host, port, factory)
    elif pid == '2'
        factory = PeerFactory('pid_2', pid)
        print "Connecting to host " + host + " port " + str(port)
        reactor.connectTCP(host, port, factory)

	reactor.run()