#pragma once

#include <string>
#include <memory>
#include <vector>
#include <functional>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>

/* unix only, no winsock */
using socket_t = int;
constexpr socket_t INVALID_SOCKET = -1;

class TCPSocket
{
public:
    TCPSocket();
    explicit TCPSocket(socket_t socket);
    ~TCPSocket();

    TCPSocket(const TCPSocket &) = delete;
    TCPSocket &operator=(const TCPSocket &) = delete;
    TCPSocket(TCPSocket &&) noexcept;
    TCPSocket &operator=(TCPSocket &&) noexcept;

    bool connect(const std::string &host, int port);
    bool bind(int port);
    bool listen(int backlog = 10);
    std::shared_ptr<TCPSocket> accept();

    int send(const std::string &data);
    int send(const void *data, size_t length);
    std::string receive(size_t maxLength = 1024);
    int receive(void *buffer, size_t length);

    std::string readLine();
    int writeString(const std::string &str);

    void close();
    bool isValid() const;

    std::string getRemoteAddress() const;
    int getRemotePort() const;

    void setBuffer(const std::string &buffer) { internalBuffer = buffer; }
    const std::string &getBuffer() const { return internalBuffer; }
    void clearBuffer() { internalBuffer.clear(); }

private:
    socket_t sock;
    std::string internalBuffer;
};

class TCPServer
{
public:
    TCPServer();
    ~TCPServer();

    TCPServer(const TCPServer &) = delete;
    TCPServer &operator=(const TCPServer &) = delete;
    TCPServer(TCPServer &&) noexcept;
    TCPServer &operator=(TCPServer &&) noexcept;

    bool start(int port, int backlog = 10);
    std::shared_ptr<TCPSocket> acceptConnection();
    void stop();

    bool isRunning() const;
    int getPort() const { return port; }

private:
    std::unique_ptr<TCPSocket> serverSocket;
    int port;
    bool running;
};