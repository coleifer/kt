class KyotoTycoonError(Exception): pass
class ImproperlyConfigured(KyotoTycoonError): pass
class ProtocolError(KyotoTycoonError): pass
class ServerConnectionError(KyotoTycoonError): pass
class ServerError(KyotoTycoonError): pass
