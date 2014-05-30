import secure_smtpd
import ssl, smtpd, asyncore, socket, logging, signal, time, sys

from smtp_channel import SMTPChannel
from asyncore import ExitNow
from process_pool import ProcessPool
from Queue import Empty
from ssl import SSLError

class SMTPServer(smtpd.SMTPServer):

    def __init__(self, localaddr, remoteaddr, ssl=False, certfile=None, keyfile=None, ssl_version=ssl.PROTOCOL_SSLv23, require_authentication=False, credential_validator=None, maximum_execution_time=30, process_count=5):
        smtpd.SMTPServer.__init__(self, localaddr, remoteaddr)
        self.logger = logging.getLogger( secure_smtpd.LOG_NAME )
        self.certfile = certfile
        self.keyfile = keyfile
        self.ssl_version = ssl_version
        self.subprocesses = []
        self.require_authentication = require_authentication
        self.credential_validator = credential_validator
        self.ssl = ssl
        self.maximum_execution_time = maximum_execution_time
        self.process_count = process_count
        self.process_pool = None
        self.newsocket = None

    def _start_channel(self,doExitNow=False,map=None):

        pair = self.accept()
        if pair is None:
            return

        self.logger.info("connection from %s" % (pair,))

        self.newsocket, fromaddr = pair
        self.newsocket.settimeout(self.maximum_execution_time)
        
        if self.ssl:
            self.newsocket = ssl.wrap_socket(
                self.newsocket,
                server_side=True,
                certfile=self.certfile,
                keyfile=self.keyfile,
                ssl_version=self.ssl_version,
                )

        channel = SMTPChannel(
            self,
            self.newsocket,
            fromaddr,
            require_authentication=self.require_authentication,
            credential_validator=self.credential_validator,
            doExitNow = doExitNow,
            map = map
            )

    def handle_accept(self):
        if (self.process_count > 1):
            self.process_pool = ProcessPool(self._accept_subprocess, process_count=self.process_count)
            self.close()
        else:
            self._start_channel()

    def handle_error(self):
        exctype,excval,exctb = sys.exc_info()
        if exctype is ssl.SSLError:
            self.logger.info("SSL Error: %s",excval)
        else:
            self.logger.error("SMTPServer: handle_error: %s %s", excval,exctb)
        if self.newsocket is not None:
            self._shutdown_socket(self.newsocket)
            self.newsocket = None
    
    def _accept_subprocess(self, queue):
        while True:
            try:
                self.socket.setblocking(1)
                self.logger.debug('_accept_subprocess(): waiting for connection.') 
                map = {}

                self._start_channel(doExitNow=True,map=map)
                self.logger.debug('_accept_subprocess(): starting asyncore loop.') 
                asyncore.loop(map=map)
                self.logger.debug('_accept_subprocess(): smtp channel terminated.') 
            except (ExitNow, SSLError):
                if self.newsocket is not None:
                    self._shutdown_socket(self.newsocket)
                    self.newsocket = None
                self.logger.debug('_accept_subprocess(): smtp channel terminated asyncore.') 
            except Exception, e:
                if self.newsocket is not None:
                    self._shutdown_socket(self.newsocket)
                    self.newsocket = None
                self.logger.error('_accept_subprocess(): uncaught exception: %s' % str(e)) 
      

    def _shutdown_socket(self, s):
        try:
            s.shutdown(socket.SHUT_RDWR)
            s.close()
        except Exception, e:
            self.logger.error('_shutdown_socket(): failed to cleanly shutdown socket: %s' % str(e))


    def run(self):
        asyncore.loop()
        if hasattr(signal, 'SIGTERM'):
            def sig_handler(signal,frame):
                self.logger.info("Got signal %s, shutting down." % signal)
                sys.exit(0)
            signal.signal(signal.SIGTERM, sig_handler)
        while 1:
            time.sleep(1)
