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

counti=0
peerlist  = []
peercount=0

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
	connected = False

	def __init__(self, factory, peer_type):
		self.pt = peer_type
		self.factory = factory

	def connectionMade(self):
		global peerlist,peercount
		peercount =peercount +1
		if self.pt == 'client':
			self.connected = True
		else:
			print "Connected from", self.transport.client
			peerlist.append(self)
			try:
				self.transport.write('<connection up>')
				reactor.callLater(5, self.sendUpdate)
			except Exception, e:
				print e.args[0]
			self.ts = time.time()

	def sendUpdate(self):
		global counti,peercount
		print "Sending update"
		try:
			
			counti=counti+1
			for i in range(0,peercount):
				if peerlist[i] != self.transport:		#elegxei se poio peer eimaste
					peerlist[i].transport.write('<update'+ " "+str(counti)+'>\n')
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

	def connectionLost(self, reason):
		print "Disconnected"
		if self.pt == 'client':
			self.connected = False
			self.done()

	def done(self):
		self.factory.finished(self.acks)


class PeerFactory(ClientFactory):

	def __init__(self, peertype, pid, fname):
		print '@__init__'
		self.pt = peertype
        self.pid = pid
		self.acks = 0
		self.fname = fname
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


if __name__ == '__main__':
	host, pid, port = parse_args()

    if pid == '0':
        factory = PeerFactory('server', pid, 'log')
        reactor.listenTCP(int(port), factory)
        print "Starting server @" + host + " port " + str(port)
    elif pid == '1':
        factory = PeerFactory('client', pid, '')
        print "Connecting to host " + host + " port " + str(port)
        reactor.connectTCP(host, port, factory)
    elif pid == '2'
        factory = PeerFactory('client', pid, '')
        print "Connecting to host " + host + " port " + str(port)
        reactor.connectTCP(host, port, factory)

	reactor.run()