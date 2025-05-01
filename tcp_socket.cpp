#include "tcp_socket.hpp"
#include <iostream>
#include <cstring>
#include <fcntl.h>
#include <errno.h>

TCPSocket::TCPSocket() : sock(INVALID_SOCKET)
{
}

TCPSocket::TCPSocket(socket_t socket) : sock(socket)
{
}

TCPSocket::~TCPSocket()
{
    close();
}

TCPSocket::TCPSocket(TCPSocket &&other) noexcept : sock(other.sock)
{
    other.sock = INVALID_SOCKET;
    internalBuffer = std::move(other.internalBuffer);
}

TCPSocket &TCPSocket::operator=(TCPSocket &&other) noexcept
{
    if (this != &other)
    {
        close();
        sock = other.sock;
        other.sock = INVALID_SOCKET;
        internalBuffer = std::move(other.internalBuffer);
    }
    return *this;
}

bool TCPSocket::connect(const std::string &host, int port)
{
    if (isValid())
    {
        close();
    }

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (!isValid())
    {
        std::cerr << "Failed to create socket: " << strerror(errno) << '\n';
        return false;
    }

    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_port = htons(port);

    struct hostent *he = gethostbyname(host.c_str());
    if (!he)
    {
        std::cerr << "Could not resolve hostname: " << host << ": " << strerror(errno) << '\n';
        close();
        return false;
    }

    std::memcpy(&server.sin_addr, he->h_addr_list[0], he->h_length);

    if (::connect(sock, (struct sockaddr *)&server, sizeof(server)) < 0)
    {
        std::cerr << "Connection failed to " << host << ":" << port << ": " << strerror(errno) << '\n';
        close();
        return false;
    }

    return true;
}

bool TCPSocket::bind(int port)
{
    if (isValid())
    {
        close();
    }

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (!isValid())
    {
        std::cerr << "Failed to create socket: " << strerror(errno) << '\n';
        return false;
    }

    /* reuse address*/
    int opt = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        std::cerr << "setsockopt(SO_REUSEADDR) failed: " << strerror(errno) << '\n';
        close();
        return false;
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (::bind(sock, (struct sockaddr *)&address, sizeof(address)) < 0)
    {
        std::cerr << "Bind failed on port " << port << ": " << strerror(errno) << '\n';
        close();
        return false;
    }

    return true;
}

bool TCPSocket::listen(int backlog)
{
    if (!isValid())
    {
        return false;
    }

    if (::listen(sock, backlog) < 0)
    {
        std::cerr << "Listen failed: " << strerror(errno) << '\n';
        return false;
    }

    return true;
}

std::shared_ptr<TCPSocket> TCPSocket::accept()
{
    if (!isValid())
    {
        return nullptr;
    }

    struct sockaddr_in client;
    socklen_t clientSize = sizeof(client);

    socket_t clientSock = ::accept(sock, (struct sockaddr *)&client, &clientSize);
    if (clientSock == INVALID_SOCKET)
    {
        std::cerr << "Accept failed: " << strerror(errno) << '\n';
        return nullptr;
    }

    return std::make_shared<TCPSocket>(clientSock);
}

int TCPSocket::send(const std::string &data)
{
    return send(data.c_str(), data.size());
}

int TCPSocket::send(const void *data, size_t length)
{
    if (!isValid())
    {
        return -1;
    }

    int result = ::send(sock, static_cast<const char *>(data), length, 0);
    if (result < 0)
    {
        std::cerr << "Send failed: " << strerror(errno) << '\n';
    }

    return result;
}

std::string TCPSocket::receive(size_t maxLength)
{
    if (!isValid())
    {
        return "";
    }

    std::vector<char> buffer(maxLength + 1);
    int bytesRead = ::recv(sock, buffer.data(), maxLength, 0);

    if (bytesRead <= 0)
    {
        if (bytesRead < 0)
        {

            if (errno != EAGAIN && errno != EWOULDBLOCK)
            {
                std::cerr << "Receive failed: " << strerror(errno) << '\n';
            }
        }
        return "";
    }

    buffer[bytesRead] = '\0';
    return std::string(buffer.data(), bytesRead);
}

int TCPSocket::receive(void *buffer, size_t length)
{
    if (!isValid())
    {
        return -1;
    }

    return ::recv(sock, static_cast<char *>(buffer), length, 0);
}

std::string TCPSocket::readLine()
{
    if (!isValid())
    {
        return "";
    }

    std::string line;
    char c;
    size_t crlfPos;

    while (true)
    {
        /* check buffer for complete line */
        crlfPos = internalBuffer.find("\r\n");
        if (crlfPos != std::string::npos)
        {
            line = internalBuffer.substr(0, crlfPos);
            internalBuffer.erase(0, crlfPos + 2);
            return line;
        }

        int result = ::recv(sock, &c, 1, 0);
        if (result <= 0)
        {

            if (result < 0)
            {
                std::cerr << "Read error: " << strerror(errno) << '\n';
            }

            if (!internalBuffer.empty())
            {
                line = internalBuffer;
                internalBuffer.clear();
                return line;
            }
            return "";
        }

        internalBuffer.push_back(c);
    }
}

int TCPSocket::writeString(const std::string &str)
{
    return send(str);
}

void TCPSocket::close()
{
    if (isValid())
    {
        ::close(sock);
        sock = INVALID_SOCKET;
    }
}

bool TCPSocket::isValid() const
{
    return sock >= 0;
}

std::string TCPSocket::getRemoteAddress() const
{
    if (!isValid())
    {
        return "";
    }

    struct sockaddr_in addr;
    socklen_t addrLen = sizeof(addr);

    if (getpeername(sock, (struct sockaddr *)&addr, &addrLen) < 0)
    {
        std::cerr << "getpeername failed: " << strerror(errno) << '\n';
        return "";
    }

    return inet_ntoa(addr.sin_addr);
}

int TCPSocket::getRemotePort() const
{
    if (!isValid())
    {
        return -1;
    }

    struct sockaddr_in addr;
    socklen_t addrLen = sizeof(addr);

    if (getpeername(sock, (struct sockaddr *)&addr, &addrLen) < 0)
    {
        std::cerr << "getpeername failed: " << strerror(errno) << '\n';
        return -1;
    }

    return ntohs(addr.sin_port);
}

TCPServer::TCPServer() : port(0), running(false)
{
}

TCPServer::~TCPServer()
{
    stop();
}

TCPServer::TCPServer(TCPServer &&other) noexcept
    : serverSocket(std::move(other.serverSocket)),
      port(other.port),
      running(other.running)
{
    other.running = false;
    other.port = 0;
}

TCPServer &TCPServer::operator=(TCPServer &&other) noexcept
{
    if (this != &other)
    {
        stop();
        serverSocket = std::move(other.serverSocket);
        port = other.port;
        running = other.running;
        other.running = false;
        other.port = 0;
    }
    return *this;
}

bool TCPServer::start(int port, int backlog)
{
    if (running)
    {
        stop();
    }

    this->port = port;
    serverSocket = std::make_unique<TCPSocket>();

    if (!serverSocket->bind(port))
    {
        serverSocket.reset();
        return false;
    }

    if (!serverSocket->listen(backlog))
    {
        serverSocket.reset();
        return false;
    }

    running = true;
    return true;
}

std::shared_ptr<TCPSocket> TCPServer::acceptConnection()
{
    if (!running || !serverSocket)
    {
        return nullptr;
    }

    return serverSocket->accept();
}

void TCPServer::stop()
{
    if (serverSocket)
    {
        serverSocket->close();
        serverSocket.reset();
    }
    running = false;
}

bool TCPServer::isRunning() const
{
    return running && serverSocket && serverSocket->isValid();
}