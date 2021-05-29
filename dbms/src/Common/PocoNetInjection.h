///
/// Using system-wised POCO Library, but we still need to add ad-hoc checks for certificates.
/// Therefore, we provide an implementation for secured sockets on ourselves.
///

#ifndef CLICKHOUSE_POCONETINJECTION_H
#define CLICKHOUSE_POCONETINJECTION_H
#ifdef TIFLASH_POCO_NET_INJECTION
#include <openssl/err.h>
#include <openssl/x509v3.h>

#include <Poco/Format.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/DNS.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/SSLException.h>
#include <Poco/Net/SecureSocketImpl.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/SecureStreamSocketImpl.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/StreamSocketImpl.h>
#include <Poco/Net/ServerSocketImpl.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/Utility.h>
#include <Poco/Net/X509Certificate.h>
#include <Poco/NumberFormatter.h>
#include <Poco/NumberParser.h>
#define POCO_BIO_set_nbio_accept(b, n) BIO_ctrl(b, BIO_C_SET_ACCEPT, 1, (void *)((n) ? "a" : NULL))

namespace Poco
{
namespace Net
{
namespace Injection
{
using Poco::InvalidArgumentException;
using Poco::IOException;
using Poco::NumberFormatter;
using Poco::TimeoutException;
using Poco::Timespan;

struct StreamSocketProxy : public SecureStreamSocket {
    StreamSocketProxy(SocketImpl* pImpl) : SecureStreamSocket(pImpl) {}
};

struct SocketImplProxy : public SocketImpl {
    template <class T>
    friend class VerifiedSecureSocketImpl;
};

struct SecureStreamSocketImplProxy : public SecureStreamSocketImpl {
    template <class T>
    friend class VerifiedSecureSocketImpl;
   
    template <typename ...Args>
    SecureStreamSocketImplProxy(Args&&...args) : SecureStreamSocketImpl(std::forward<Args>(args)...){}
};


static inline bool isLocalHost(const std::string& hostName)
{
	try
	{
		SocketAddress addr(hostName, 0);
		return addr.host().isLoopback();
	}
	catch (Poco::Exception&)
	{
		return false;
	}
}

template <class Verifier>
class VerifiedSecureSocketImpl {
public:
    VerifiedSecureSocketImpl(Verifier verifier, Poco::AutoPtr<SocketImpl> pSocketImpl, Context::Ptr pContext);
    virtual ~VerifiedSecureSocketImpl();
    SocketImpl * acceptConnection(SocketAddress & clientAddr);
    void connect(const SocketAddress & address, bool performHandshake);
    void connect(const SocketAddress & address, const Poco::Timespan & timeout, bool performHandshake);
    void connectNB(const SocketAddress & address);
    void bind(const SocketAddress & address, bool reuseAddress = false, bool reusePort = false);
    void listen(int backlog = 64);
    void shutdown();
    void close();
    void abort();
    int sendBytes(const void * buffer, int length, int flags = 0);
    int receiveBytes(void * buffer, int length, int flags = 0);
    int available() const;
    int completeHandshake();
    poco_socket_t sockfd();
    X509 * peerCertificate() const;
    Context::Ptr context() const;
    void verifyPeerCertificate();
    void verifyPeerCertificate(const std::string & hostName);
    void setPeerHostName(const std::string & hostName);
    const std::string & getPeerHostName() const;
    Session::Ptr currentSession();
    void useSession(Session::Ptr pSession);
    bool sessionWasReused();
protected:
    void acceptSSL();
    void connectSSL(bool performHandshake);
    long verifyPeerCertificateImpl(const std::string & hostName);
    bool mustRetry(int rc);
    int handleError(int rc);
    void reset();

    VerifiedSecureSocketImpl(const VerifiedSecureSocketImpl &) = delete;
    VerifiedSecureSocketImpl & operator=(const VerifiedSecureSocketImpl &) = delete;
private:
    SSL * _pSSL;
    Poco::AutoPtr<SocketImpl> _pSocket;
    Context::Ptr _pContext;
    bool _needHandshake;
    std::string _peerHostName;
    Session::Ptr _pSession;
    Verifier _verifier;
};




template <typename Verifier>
VerifiedSecureSocketImpl<Verifier>::VerifiedSecureSocketImpl(Verifier verifier, Poco::AutoPtr<SocketImpl> pSocketImpl, Context::Ptr pContext):
	_pSSL(0),
	_pSocket(pSocketImpl),
	_pContext(pContext),
	_needHandshake(false),
        _verifier(std::move(verifier))
{
	poco_check_ptr (_pSocket);
	poco_check_ptr (_pContext);
}

template<typename Verifier>
VerifiedSecureSocketImpl<Verifier>::~VerifiedSecureSocketImpl()
{
	try
	{
		reset();
	}
	catch (...)
	{
		poco_unexpected();
	}
}

template <typename Verifier>
SocketImpl* VerifiedSecureSocketImpl<Verifier>::acceptConnection(SocketAddress& clientAddr)
{
	poco_assert (!_pSSL);

	StreamSocket ss = _pSocket->acceptConnection(clientAddr);
	Poco::AutoPtr<SecureStreamSocketImplProxy> pSecureStreamSocketImplProxy = new SecureStreamSocketImplProxy(static_cast<StreamSocketImpl*>(ss.impl()), _pContext);
	pSecureStreamSocketImplProxy->acceptSSL();
	pSecureStreamSocketImplProxy->duplicate();
	return pSecureStreamSocketImplProxy;
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::acceptSSL()
{
	poco_assert (!_pSSL);

	BIO* pBIO = BIO_new(BIO_s_socket());
	if (!pBIO) throw SSLException("Cannot create BIO object");
	BIO_set_fd(pBIO, static_cast<int>(_pSocket->sockfd()), BIO_NOCLOSE);

	_pSSL = SSL_new(_pContext->sslContext());
	if (!_pSSL)
	{
		BIO_free(pBIO);
		throw SSLException("Cannot create SSL object");
	}
	SSL_set_bio(_pSSL, pBIO, pBIO);
	SSL_set_accept_state(_pSSL);
	_needHandshake = true;
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::connect(const SocketAddress& address, bool performHandshake)
{
	if (_pSSL) reset();

	poco_assert (!_pSSL);

	_pSocket->connect(address);
	connectSSL(performHandshake);
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::connect(const SocketAddress& address, const Poco::Timespan& timeout, bool performHandshake)
{
	if (_pSSL) reset();

	poco_assert (!_pSSL);

	_pSocket->connect(address, timeout);
	Poco::Timespan receiveTimeout = _pSocket->getReceiveTimeout();
	Poco::Timespan sendTimeout = _pSocket->getSendTimeout();
	_pSocket->setReceiveTimeout(timeout);
	_pSocket->setSendTimeout(timeout);
	connectSSL(performHandshake);
	_pSocket->setReceiveTimeout(receiveTimeout);
	_pSocket->setSendTimeout(sendTimeout);
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::connectNB(const SocketAddress& address)
{
	if (_pSSL) reset();

	poco_assert (!_pSSL);

	_pSocket->connectNB(address);
	connectSSL(false);
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::connectSSL(bool performHandshake)
{
	poco_assert (!_pSSL);
	poco_assert (_pSocket->initialized());

	BIO* pBIO = BIO_new(BIO_s_socket());
	if (!pBIO) throw SSLException("Cannot create SSL BIO object");
	BIO_set_fd(pBIO, static_cast<int>(_pSocket->sockfd()), BIO_NOCLOSE);

	_pSSL = SSL_new(_pContext->sslContext());
	if (!_pSSL)
	{
		BIO_free(pBIO);
		throw SSLException("Cannot create SSL object");
	}
	SSL_set_bio(_pSSL, pBIO, pBIO);

#if OPENSSL_VERSION_NUMBER >= 0x0908060L && !defined(OPENSSL_NO_TLSEXT)
	if (!_peerHostName.empty())
	{
		SSL_set_tlsext_host_name(_pSSL, _peerHostName.c_str());
	}
#endif

	if (_pSession)
	{
		SSL_set_session(_pSSL, _pSession->sslSession());
	}

	try
	{
		if (performHandshake && _pSocket->getBlocking())
		{
			int ret = SSL_connect(_pSSL);
			handleError(ret);
			verifyPeerCertificate();
		}
		else
		{
			SSL_set_connect_state(_pSSL);
			_needHandshake = true;
		}
	}
	catch (...)
	{
		SSL_free(_pSSL);
		_pSSL = 0;
		throw;
	}
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::bind(const SocketAddress& address, bool reuseAddress, bool reusePort)
{
	poco_check_ptr (_pSocket);

	_pSocket->bind(address, reuseAddress, reusePort);
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::listen(int backlog)
{
	poco_check_ptr (_pSocket);

	_pSocket->listen(backlog);
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::shutdown()
{
	if (_pSSL)
	{
        // Don't shut down the socket more than once.
        int shutdownState = SSL_get_shutdown(_pSSL);
        bool shutdownSent = (shutdownState & SSL_SENT_SHUTDOWN) == SSL_SENT_SHUTDOWN;
        if (!shutdownSent)
        {
			// A proper clean shutdown would require us to
			// retry the shutdown if we get a zero return
			// value, until SSL_shutdown() returns 1.
			// However, this will lead to problems with
			// most web browsers, so we just set the shutdown
			// flag by calling SSL_shutdown() once and be
			// done with it.
			int rc = SSL_shutdown(_pSSL);
			if (rc < 0) handleError(rc);
			if (_pSocket->getBlocking())
			{
				_pSocket->shutdown();
			}
		}
	}
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::close()
{
	try
	{
		shutdown();
	}
	catch (...)
	{
	}
	_pSocket->close();
}


template <typename Verifier>
int VerifiedSecureSocketImpl<Verifier>::sendBytes(const void* buffer, int length, int flags)
{
	poco_assert (_pSocket->initialized());
	poco_check_ptr (_pSSL);

	int rc;
	if (_needHandshake)
	{
		rc = completeHandshake();
		if (rc == 1)
			verifyPeerCertificate();
		else if (rc == 0)
			throw SSLConnectionUnexpectedlyClosedException();
		else
			return rc;
	}
	do
	{
		rc = SSL_write(_pSSL, buffer, length);
	}
	while (mustRetry(rc));
	if (rc <= 0)
	{
		rc = handleError(rc);
		if (rc == 0) throw SSLConnectionUnexpectedlyClosedException();
	}
	return rc;
}


template <typename Verifier>
int VerifiedSecureSocketImpl<Verifier>::receiveBytes(void* buffer, int length, int flags)
{
	poco_assert (_pSocket->initialized());
	poco_check_ptr (_pSSL);

	int rc;
	if (_needHandshake)
	{
		rc = completeHandshake();
		if (rc == 1)
			verifyPeerCertificate();
		else
			return rc;
	}
	do
	{
		rc = SSL_read(_pSSL, buffer, length);
	}
	while (mustRetry(rc));
	if (rc <= 0)
	{
		return handleError(rc);
	}
	return rc;
}


template <typename Verifier>
int VerifiedSecureSocketImpl<Verifier>::available() const
{
	poco_check_ptr (_pSSL);

	return SSL_pending(_pSSL);
}


template <typename Verifier>
int VerifiedSecureSocketImpl<Verifier>::completeHandshake()
{
	poco_assert (_pSocket->initialized());
	poco_check_ptr (_pSSL);

	int rc;
	do
	{
		rc = SSL_do_handshake(_pSSL);
	}
	while (mustRetry(rc));
	if (rc <= 0)
	{
		return handleError(rc);
	}
	_needHandshake = false;
	return rc;
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::verifyPeerCertificate()
{
	if (_peerHostName.empty())
		verifyPeerCertificate(_pSocket->peerAddress().host().toString());
	else
		verifyPeerCertificate(_peerHostName);
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::verifyPeerCertificate(const std::string& hostName)
{
	long certErr = verifyPeerCertificateImpl(hostName);
	if (certErr != X509_V_OK)
	{
		std::string msg = Utility::convertCertificateError(certErr);
		throw CertificateValidationException("Unacceptable certificate from " + hostName, msg);
	}
}


template <typename Verifier>
long VerifiedSecureSocketImpl<Verifier>::verifyPeerCertificateImpl(const std::string& hostName)
{
	Context::VerificationMode mode = _pContext->verificationMode();
	if (mode == Context::VERIFY_NONE || !_pContext->extendedCertificateVerificationEnabled() ||
	    (mode != Context::VERIFY_STRICT && isLocalHost(hostName)))
	{
		return X509_V_OK;
	}

	X509* pCert = SSL_get_peer_certificate(_pSSL);
	if (pCert)
	{
		X509Certificate cert(pCert);
		return cert.verify(hostName) && _verifier(pCert) ? X509_V_OK : X509_V_ERR_APPLICATION_VERIFICATION;
	}
	else return X509_V_OK;
}

template <typename Verifier>
X509* VerifiedSecureSocketImpl<Verifier>::peerCertificate() const
{
	if (_pSSL)
		return SSL_get_peer_certificate(_pSSL);
	else
		return 0;
}


template <typename Verifier>
bool VerifiedSecureSocketImpl<Verifier>::mustRetry(int rc)
{
	if (rc <= 0)
	{
		int sslError = SSL_get_error(_pSSL, rc);
		int socketError = _pSocket->lastError();
		switch (sslError)
		{
		case SSL_ERROR_WANT_READ:
			if (_pSocket->getBlocking())
			{
				if (_pSocket->poll(_pSocket->getReceiveTimeout(), Poco::Net::Socket::SELECT_READ))
					return true;
				else
					throw Poco::TimeoutException();
			}
			break;
		case SSL_ERROR_WANT_WRITE:
			if (_pSocket->getBlocking())
			{
				if (_pSocket->poll(_pSocket->getSendTimeout(), Poco::Net::Socket::SELECT_WRITE))
					return true;
				else
					throw Poco::TimeoutException();
			}
			break;
		case SSL_ERROR_SYSCALL:
			return socketError == POCO_EAGAIN || socketError == POCO_EINTR;
		default:
			return socketError == POCO_EINTR;
		}
	}
	return false;
}


template <typename Verifier>
int VerifiedSecureSocketImpl<Verifier>::handleError(int rc)
{
	if (rc > 0) return rc;

	int sslError = SSL_get_error(_pSSL, rc);
	int error = SocketImplProxy::lastError();

	switch (sslError)
	{
	case SSL_ERROR_ZERO_RETURN:
		return 0;
	case SSL_ERROR_WANT_READ:
		return SecureStreamSocket::ERR_SSL_WANT_READ;
	case SSL_ERROR_WANT_WRITE:
		return SecureStreamSocket::ERR_SSL_WANT_WRITE;
	case SSL_ERROR_WANT_CONNECT:
	case SSL_ERROR_WANT_ACCEPT:
	case SSL_ERROR_WANT_X509_LOOKUP:
		// these should not occur
		poco_bugcheck();
		return rc;
	case SSL_ERROR_SYSCALL:
		if (error != 0)
		{
			SocketImpl::error(error);
		}
		// fallthrough
	default:
		{
			long lastError = ERR_get_error();
			if (lastError == 0)
			{
				if (rc == 0)
				{
					// Most web browsers do this, don't report an error
					if (_pContext->isForServerUse())
						return 0;
					else
						throw SSLConnectionUnexpectedlyClosedException();
				}
				else if (rc == -1)
				{
					throw SSLConnectionUnexpectedlyClosedException();
				}
				else
				{
					SecureStreamSocketImplProxy::error(Poco::format("The BIO reported an error: %d", rc));
				}
			}
			else
			{
				char buffer[256];
				ERR_error_string_n(lastError, buffer, sizeof(buffer));
				std::string msg(buffer);
				throw SSLException(msg);
			}
		}
 		break;
	}
	return rc;
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::setPeerHostName(const std::string& peerHostName)
{
	_peerHostName = peerHostName;
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::reset()
{
	close();
	if (_pSSL)
	{
		SSL_free(_pSSL);
		_pSSL = 0;
	}
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::abort()
{
	_pSocket->shutdown();
}


template <typename Verifier>
Session::Ptr VerifiedSecureSocketImpl<Verifier>::currentSession()
{
	if (_pSSL)
	{
		SSL_SESSION* pSession = SSL_get1_session(_pSSL);
		if (pSession)
		{
			if (_pSession && pSession == _pSession->sslSession())
			{
				SSL_SESSION_free(pSession);
				return _pSession;
			}
			else return new Session(pSession);
		}
	}
	return 0;
}


template <typename Verifier>
void VerifiedSecureSocketImpl<Verifier>::useSession(Session::Ptr pSession)
{
	_pSession = pSession;
}


template <typename Verifier>
bool VerifiedSecureSocketImpl<Verifier>::sessionWasReused()
{
	if (_pSSL)
		return SSL_session_reused(_pSSL) != 0;
	else
		return false;
}

template <typename Verifier>
poco_socket_t VerifiedSecureSocketImpl<Verifier>::sockfd()
{
	return _pSocket->sockfd();
}

template<typename Verifier>
inline Context::Ptr VerifiedSecureSocketImpl<Verifier>::context() const
{
	return _pContext;
}

template<typename Verifier>
inline const std::string& VerifiedSecureSocketImpl<Verifier>::getPeerHostName() const
{
	return _peerHostName;
}

template <class Verifier>
class NetSSL_API VerifiedSecureServerSocketImpl : public ServerSocketImpl
{
public:
    VerifiedSecureServerSocketImpl(Verifier verifier, Poco::Net::Context::Ptr pContext)
        : _impl(std::move(verifier), new ServerSocketImpl, pContext){};


    SocketImpl * acceptConnection(SocketAddress & clientAddr) { return _impl.acceptConnection(clientAddr); };

    void connect(const SocketAddress &) { throw Poco::InvalidAccessException("Cannot connect() a SecureServerSocket"); };

    void connect(const SocketAddress &, const Poco::Timespan &)
    {
        throw Poco::InvalidAccessException("Cannot connect() a SecureServerSocket");
    };

    void connectNB(const SocketAddress &) { throw Poco::InvalidAccessException("Cannot connect() a SecureServerSocket"); };

    void bind(const SocketAddress & address, bool reuseAddress = false, bool reusePort = false)
    {
        _impl.bind(address, reuseAddress, reusePort);
        reset(_impl.sockfd());
    }

    void listen(int backlog = 64)
    {
        _impl.listen(backlog);
        reset(_impl.sockfd());
    };

    void close()
    {
        reset();
        _impl.close();
    };

    int sendBytes(const void *, int, int = 0)
    {
        throw Poco::InvalidAccessException("Cannot sendBytes() on a SecureServerSocket");
    };

    int receiveBytes(void *, int, int = 0)
    {
        throw Poco::InvalidAccessException("Cannot receiveBytes() on a SecureServerSocket");
    };
    int sendTo(const void *, int, const SocketAddress &, int = 0)
    {
        throw Poco::InvalidAccessException("Cannot sendTo() on a SecureServerSocket");
    };
    int receiveFrom(void *, int, SocketAddress &, int = 0)
    {
        throw Poco::InvalidAccessException("Cannot receiveFrom() on a SecureServerSocket");
    };
    void sendUrgent(unsigned char) { throw Poco::InvalidAccessException("Cannot sendUrgent() on a SecureServerSocket"); };
    bool secure() const { return true; };

    Context::Ptr context() const { return _impl.context(); };

protected:
    ~VerifiedSecureServerSocketImpl()
    {
        try
        {
            reset();
        }
        catch (...)
        {
            poco_unexpected();
        }
    }

public:
    VerifiedSecureServerSocketImpl(const VerifiedSecureServerSocketImpl &) = delete;
    VerifiedSecureServerSocketImpl & operator=(const VerifiedSecureServerSocketImpl &) = delete;

private:
    VerifiedSecureSocketImpl<Verifier> _impl;
};

template <class Verifier>
class NetSSL_API VerifiedSecureServerSocket : public ServerSocket
{
public:
    VerifiedSecureServerSocket(Verifier verifier)
        : ServerSocket(new VerifiedSecureServerSocketImpl<Verifier>(std::move(verifier), SSLManager::instance().defaultClientContext()), true){};

    explicit VerifiedSecureServerSocket(Verifier verifier, Context::Ptr pContext)
        : ServerSocket(new VerifiedSecureServerSocketImpl<Verifier>(std::move(verifier), pContext), true) {};

    VerifiedSecureServerSocket(const Socket & socket) : ServerSocket(socket)
    {
        if (!dynamic_cast<VerifiedSecureServerSocketImpl<Verifier> *>(impl()))
            throw Poco::InvalidArgumentException("Cannot assign incompatible socket");
    };

    VerifiedSecureServerSocket(Verifier verifier, const SocketAddress & address, int backlog = 64)
        : ServerSocket(new VerifiedSecureServerSocketImpl<Verifier>(std::move(verifier), SSLManager::instance().defaultClientContext()), true)
    {
        impl()->bind(address, true);
        impl()->listen(backlog);
    };
    VerifiedSecureServerSocket(Verifier verifier, const SocketAddress & address, int backlog, Context::Ptr pContext)
        : ServerSocket(new VerifiedSecureServerSocketImpl<Verifier>(std::move(verifier), pContext), true)
    {
        impl()->bind(address, true);
        impl()->listen(backlog);
    };
    VerifiedSecureServerSocket(Verifier verifier, Poco::UInt16 port, int backlog = 64)
        : ServerSocket(new VerifiedSecureServerSocketImpl<Verifier>(std::move(verifier), SSLManager::instance().defaultClientContext()), true)
    {
        IPAddress wildcardAddr;
        SocketAddress address(wildcardAddr, port);
        impl()->bind(address, true);
        impl()->listen(backlog);
    };
    VerifiedSecureServerSocket(Verifier verifier, Poco::UInt16 port, int backlog, Context::Ptr pContext)
        : ServerSocket(new VerifiedSecureServerSocketImpl<Verifier>(std::move(verifier), pContext), true)
    {
        IPAddress wildcardAddr;
        SocketAddress address(wildcardAddr, port);
        impl()->bind(address, true);
        impl()->listen(backlog);
    };
    virtual ~VerifiedSecureServerSocket() = default;

    VerifiedSecureServerSocket & operator=(const Socket & socket)
    {
        if (&socket != this)
        {
            if (dynamic_cast<VerifiedSecureServerSocketImpl<Verifier> *>(socket.impl()))
                ServerSocket::operator=(socket);
            else
                throw Poco::InvalidArgumentException("Cannot assign incompatible socket");
        }
        return *this;
    };
    StreamSocket acceptConnection(SocketAddress & clientAddr)
    {
        return StreamSocketProxy(impl()->acceptConnection(clientAddr));
    };
    StreamSocket acceptConnection()
    {
        SocketAddress clientAddr;
        return acceptConnection(clientAddr);
    }
    Context::Ptr context() const { return static_cast<VerifiedSecureServerSocketImpl<Verifier> *>(impl())->context(); };
};




} // namespace Injection
} // namespace Net
} // namespace Poco

#undef POCO_BIO_set_nbio_accept
#endif
#endif //CLICKHOUSE_POCONETINJECTION_H
