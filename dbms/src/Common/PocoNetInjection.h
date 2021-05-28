//
// Created by schrodinger on 5/28/21.
//

#ifndef CLICKHOUSE_POCONETINJECTION_H
#define CLICKHOUSE_POCONETINJECTION_H

#include <Poco/Exception.h>
#include <Poco/Net/SSLException.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureServerSocketImpl.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/Utility.h>

namespace PocoInjection
{
using namespace Poco::Net;


static inline bool isLocalHost(const std::string & hostName)
{
    try
    {
        SocketAddress addr(hostName, 0);
        return addr.host().isLoopback();
    }
    catch (Poco::Exception &)
    {
        return false;
    }
}

template <class Verifier>
class NetSSL_API VerifiedSecureSocketImpl
{
    SecureSocketImpl _impl;
    Verifier verifier;

public:
    VerifiedSecureSocketImpl(Verifier verifier, Poco::AutoPtr<SocketImpl> pSocketImpl, Poco::Net::Context::Ptr pContext)
        : _impl(std::move(pSocketImpl), std::move(pContext)), verifier(std::move(verifier)){};
    ~VerifiedSecureSocketImpl() = default;
    SocketImpl * acceptConnection(SocketAddress & clientAddr) { return _impl.acceptConnection(clientAddr); };
    void connect(const SocketAddress & address, bool performHandshake) { _impl.connect(address, performHandshake); };
    void connect(const SocketAddress & address, const Poco::Timespan & timeout, bool performHandshake)
    {
        _impl.connect(address, timeout, performHandshake);
    };
    void connectNB(const SocketAddress & address) { _impl.connectNB(address); };
    void bind(const SocketAddress & address, bool reuseAddress = false, bool reusePort = false)
    {
        _impl.bind(address, reuseAddress, reusePort);
    };
    void listen(int backlog = 64) { _impl.listen(backlog); };
    void shutdown() { _impl.shutdown(); };
    void close() { _impl.close(); };
    void abort() { _impl.abort(); };
    int sendBytes(const void * buffer, int length, int flags = 0) { return _impl.sendBytes(buffer, length, flags); };
    int receiveBytes(void * buffer, int length, int flags = 0) { return _impl.receiveBytes(buffer, length, flags); };
    int available() const { return _impl.available(); };
    int completeHandshake() { return _impl.completeHandshake(); };
    poco_socket_t sockfd() { return _impl.completeHandshake(); };
    X509 * peerCertificate() const { return _impl.peerCertificate(); };
    Poco::Net::Context::Ptr context() const { return _impl.context(); };

    void verifyPeerCertificate()
    {
        if (peerCertificate() && !verifier(peerCertificate()))
        {
            throw CertificateValidationException("Unacceptable certificate from " + getPeerHostName(),
                Poco::Net::Utility::convertCertificateError(X509_V_ERR_APPLICATION_VERIFICATION));
        }
        _impl.verifyPeerCertificate();
    };

    void verifyPeerCertificate(const std::string & hostName)
    {
        if (peerCertificate() && !verifier(peerCertificate()))
        {
            throw CertificateValidationException("Unacceptable certificate from " + getPeerHostName(),
                Poco::Net::Utility::convertCertificateError(X509_V_ERR_APPLICATION_VERIFICATION));
        }
        _impl.verifyPeerCertificate(hostName);
    };
    void setPeerHostName(const std::string & hostName) { _impl.setPeerHostName(hostName); };
    const std::string & getPeerHostName() const { return _impl.getPeerHostName(); };
    Session::Ptr currentSession() { return _impl.currentSession(); };
    void useSession(Session::Ptr pSession) { _impl.useSession(pSession); };
    bool sessionWasReused() { return _impl.sessionWasReused(); };
};


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

struct StreamSocketProxy : public SecureStreamSocket {
    StreamSocketProxy(SocketImpl* pImpl) : SecureStreamSocket(pImpl) {}
};

template <class Verifier>
class NetSSL_API VerifiedSecureServerSocket : public ServerSocket
{
public:
    VerifiedSecureServerSocket(Verifier verifier)
        : ServerSocket(new VerifiedSecureServerSocketImpl(std::move(verifier), SSLManager::instance().defaultClientContext()), true){};

    explicit VerifiedSecureServerSocket(Verifier verifier, Context::Ptr pContext)
        : ServerSocket(new VerifiedSecureServerSocketImpl(std::move(verifier), pContext), true) {};

    VerifiedSecureServerSocket(const Socket & socket) : ServerSocket(socket)
    {
        if (!dynamic_cast<VerifiedSecureServerSocketImpl<Verifier> *>(impl()))
            throw Poco::InvalidArgumentException("Cannot assign incompatible socket");
    };

    VerifiedSecureServerSocket(Verifier verifier, const SocketAddress & address, int backlog = 64)
        : ServerSocket(new VerifiedSecureServerSocketImpl(std::move(verifier), SSLManager::instance().defaultClientContext()), true)
    {
        impl()->bind(address, true);
        impl()->listen(backlog);
    };
    VerifiedSecureServerSocket(Verifier verifier, const SocketAddress & address, int backlog, Context::Ptr pContext)
        : ServerSocket(new VerifiedSecureServerSocketImpl(std::move(verifier), pContext), true)
    {
        impl()->bind(address, true);
        impl()->listen(backlog);
    };
    VerifiedSecureServerSocket(Verifier verifier, Poco::UInt16 port, int backlog = 64)
        : ServerSocket(new VerifiedSecureServerSocketImpl(std::move(verifier), SSLManager::instance().defaultClientContext()), true)
    {
        IPAddress wildcardAddr;
        SocketAddress address(wildcardAddr, port);
        impl()->bind(address, true);
        impl()->listen(backlog);
    };
    VerifiedSecureServerSocket(Verifier verifier, Poco::UInt16 port, int backlog, Context::Ptr pContext)
        : ServerSocket(new VerifiedSecureServerSocketImpl(std::move(verifier), pContext), true)
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
} // namespace PocoInjection

#endif //CLICKHOUSE_POCONETINJECTION_H
