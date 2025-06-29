#include "event_loop.hpp"
#include "server.hpp"
#include "tcp_socket.hpp"
#include "resp.hpp"
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <cstring>

EventLoop::EventLoop(ServerState* server) : server(server), kq(-1), serverFd(-1) {
    kq = kqueue();
    if (kq == -1) {
        throw std::runtime_error("Failed to create kqueue");
    }
}

EventLoop::~EventLoop() {
    stop();
    if (kq != -1) {
        close(kq);
    }
}

void EventLoop::run() {
    // Set up server socket manually since we need non-blocking
    serverFd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd < 0) {
        throw std::runtime_error("Failed to create server socket");
    }

    // Reuse address
    int opt = 1;
    if (setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        close(serverFd);
        throw std::runtime_error("Failed to set SO_REUSEADDR");
    }

    // Bind
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(server->getConfig().port);

    if (bind(serverFd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        close(serverFd);
        throw std::runtime_error("Failed to bind server socket");
    }

    // Listen
    if (listen(serverFd, 10) < 0) {
        close(serverFd);
        throw std::runtime_error("Failed to listen on server socket");
    }

    // Make server socket non-blocking
    int flags = fcntl(serverFd, F_GETFL, 0);
    fcntl(serverFd, F_SETFL, flags | O_NONBLOCK);

    // Register server socket with kqueue
    struct kevent ev;
    EV_SET(&ev, serverFd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
    if (kevent(kq, &ev, 1, nullptr, 0, nullptr) < 0) {
        close(serverFd);
        throw std::runtime_error("Failed to register server socket with kqueue");
    }

    running = true;
    std::cout << "Event loop running on port " << server->getConfig().port << std::endl;

    const int MAX_EVENTS = 64;
    struct kevent events[MAX_EVENTS];

    while (running) {
        // Poll for events with 100ms timeout
        struct timespec timeout = {0, 100000000}; // 100ms
        int nev = kevent(kq, nullptr, 0, events, MAX_EVENTS, &timeout);

        if (nev < 0) {
            std::cerr << "kevent error: " << strerror(errno) << std::endl;
            break;
        }

        // Process events
        for (int i = 0; i < nev; i++) {
            const struct kevent& ev = events[i];

            if (ev.ident == static_cast<uintptr_t>(serverFd)) {
                handleServerRead();
            } else {
                if (ev.filter == EVFILT_READ) {
                    handleClientRead(ev.ident);
                } else if (ev.filter == EVFILT_WRITE) {
                    handleClientWrite(ev.ident);
                }
            }
        }

        // Handle timeouts and pending responses
        checkTimeouts();
        processPendingResponses();
    }
}

void EventLoop::stop() {
    running = false;
    if (serverFd != -1) {
        close(serverFd);
        serverFd = -1;
    }

    // Close all client connections
    for (auto& [fd, client] : clients) {
        close(fd);
    }
    clients.clear();
}

void EventLoop::handleServerRead() {
    // Accept new connections
    struct sockaddr_in clientAddr;
    socklen_t clientLen = sizeof(clientAddr);

    while (true) {
        int clientFd = accept(serverFd, (struct sockaddr*)&clientAddr, &clientLen);
        if (clientFd < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                std::cerr << "Accept error: " << strerror(errno) << std::endl;
            }
            break;
        }

        addClient(clientFd);
    }
}

void EventLoop::addClient(int fd) {
    // Make client socket non-blocking
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    // Register with kqueue for read events
    struct kevent ev;
    EV_SET(&ev, fd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
    if (kevent(kq, &ev, 1, nullptr, 0, nullptr) < 0) {
        std::cerr << "Failed to register client with kqueue: " << strerror(errno) << std::endl;
        close(fd);
        return;
    }

    // Create client connection
    clients[fd] = std::make_unique<ClientConnection>();
    clients[fd]->fd = fd;

    std::cout << "Client connected: fd=" << fd << std::endl;
}

void EventLoop::removeClient(int fd) {
    auto it = clients.find(fd);
    if (it == clients.end()) return;

    // Remove from any blocked lists
    for (auto& [stream, clientList] : blockedClients) {
        clientList.erase(
            std::remove(clientList.begin(), clientList.end(), fd),
            clientList.end()
        );
    }

    // Remove from waiting clients
    waitingClients.erase(
        std::remove_if(waitingClients.begin(), waitingClients.end(),
                      [fd](const WaitingClient& wc) { return wc.clientId == fd; }),
        waitingClients.end()
    );

    // Remove from kqueue and close
    struct kevent ev[2];
    EV_SET(&ev[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    EV_SET(&ev[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    kevent(kq, ev, 2, nullptr, 0, nullptr);

    close(fd);
    clients.erase(it);

    std::cout << "Client disconnected: fd=" << fd << std::endl;
}

void EventLoop::handleClientRead(int fd) {
    auto it = clients.find(fd);
    if (it == clients.end()) return;

    auto& client = it->second;

    // Read data
    char buffer[1024];
    ssize_t bytes = recv(fd, buffer, sizeof(buffer), 0);

    if (bytes <= 0) {
        if (bytes == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
            removeClient(fd);
        }
        return;
    }

    client->readBuffer.append(buffer, bytes);

    // Try to parse complete commands
    size_t pos = 0;
    while (pos < client->readBuffer.size()) {
        try {
            auto [cmd, bytesRead] = RESPProtocol::decodeStringArray(
                client->readBuffer.substr(pos));

            if (cmd.empty()) break; // Incomplete command

            pos += bytesRead;
            processCommand(fd, cmd);

        } catch (const std::exception& e) {
            // Incomplete command, wait for more data
            break;
        }
    }

    // Remove processed data
    client->readBuffer.erase(0, pos);
}

void EventLoop::handleClientWrite(int fd) {
    auto it = clients.find(fd);
    if (it == clients.end()) return;

    auto& client = it->second;

    if (client->writeBuffer.empty()) {
        // Disable write events
        struct kevent ev;
        EV_SET(&ev, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        kevent(kq, &ev, 1, nullptr, 0, nullptr);
        return;
    }

    ssize_t bytes = send(fd, client->writeBuffer.data() + client->writePos,
                        client->writeBuffer.size() - client->writePos, 0);

    if (bytes < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            removeClient(fd);
        }
        return;
    }

    client->writePos += bytes;

    if (client->writePos >= client->writeBuffer.size()) {
        // All data sent
        client->writeBuffer.clear();
        client->writePos = 0;

        // Disable write events
        struct kevent ev;
        EV_SET(&ev, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        kevent(kq, &ev, 1, nullptr, 0, nullptr);
    }
}

void EventLoop::processCommand(int clientId, const std::vector<std::string>& cmd) {
    auto& client = clients[clientId];

    // Handle transaction commands locally
    if (cmd[0] == "EXEC" && client->inMulti) {
        std::vector<std::string> responses;
        for (const auto& queuedCmd : client->queuedCommands) {
            std::string response = server->handleCommand(queuedCmd, clientId);
            responses.push_back(response);
        }
        std::string response = RESPProtocol::encodeArray(responses);
        scheduleResponse(clientId, response);

        client->queuedCommands.clear();
        client->inMulti = false;
        return;
    }

    if (cmd[0] == "DISCARD" && client->inMulti) {
        client->queuedCommands.clear();
        client->inMulti = false;
        scheduleResponse(clientId, RESPProtocol::encodeSimpleString("OK"));
        return;
    }

    if (client->inMulti && cmd[0] != "MULTI") {
        client->queuedCommands.push_back(cmd);
        scheduleResponse(clientId, RESPProtocol::encodeSimpleString("QUEUED"));
        return;
    }

    if (cmd[0] == "MULTI") {
        client->inMulti = true;
        scheduleResponse(clientId, RESPProtocol::encodeSimpleString("OK"));
        return;
    }

    // Handle XREAD BLOCK specially
    if (cmd[0] == "XREAD" && cmd.size() > 4 && cmd[1] == "BLOCK") {
        int timeout = std::stoi(cmd[2]);
        // Find STREAMS keyword
        size_t streamsIdx = 0;
        for (size_t i = 3; i < cmd.size(); i++) {
            if (cmd[i] == "STREAMS") {
                streamsIdx = i;
                break;
            }
        }

        if (streamsIdx > 0 && streamsIdx + 2 < cmd.size()) {
            std::string stream = cmd[streamsIdx + 1];
            std::string startId = cmd[streamsIdx + 2];

            // Check if data is immediately available
            std::string response = server->handleCommand(cmd, clientId);
            if (response != "*0\r\n" && response != "$-1\r\n") {
                // Data available, send immediately
                scheduleResponse(clientId, response);
            } else {
                // No data, block client
                blockClient(clientId, stream, startId, timeout);
            }
        }
        return;
    }

    // Regular command processing
    std::string response = server->handleCommand(cmd, clientId);
    if (!response.empty()) {
        scheduleResponse(clientId, response);
    }
}

void EventLoop::scheduleResponse(int clientId, const std::string& response) {
    auto it = clients.find(clientId);
    if (it == clients.end()) return;

    auto& client = it->second;
    bool wasEmpty = client->writeBuffer.empty();
    client->writeBuffer += response;

    // Enable write events for this client if buffer was empty
    if (wasEmpty) {
        struct kevent ev;
        EV_SET(&ev, clientId, EVFILT_WRITE, EV_ADD, 0, 0, nullptr);
        kevent(kq, &ev, 1, nullptr, 0, nullptr);
    }
}

void EventLoop::blockClient(int clientId, const std::string& stream,
                           const std::string& startId, int timeoutMs) {
    auto& client = clients[clientId];
    client->isBlocked = true;
    client->blockedOnStream = stream;
    client->blockStartId = startId;

    if (timeoutMs > 0) {
        client->blockTimeout = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeoutMs);
    }

    blockedClients[stream].push_back(clientId);
}

void EventLoop::notifyStreamClients(const std::string& stream) {
    auto it = blockedClients.find(stream);
    if (it == blockedClients.end()) return;

    for (int clientId : it->second) {
        auto clientIt = clients.find(clientId);
        if (clientIt != clients.end()) {
            auto& client = clientIt->second;
            client->isBlocked = false;

            // Generate response for this client
            std::vector<std::string> xreadCmd = {
                "XREAD", "STREAMS", stream, client->blockStartId
            };
            std::string response = server->handleCommand(xreadCmd, clientId);
            scheduleResponse(clientId, response);
        }
    }

    it->second.clear();
}

void EventLoop::addWaitingClient(int clientId, int needAcks, int timeoutMs) {
    WaitingClient wc;
    wc.clientId = clientId;
    wc.needAcks = needAcks;
    wc.receivedAcks = 0;

    if (timeoutMs > 0) {
        wc.timeout = std::chrono::steady_clock::now() +
                     std::chrono::milliseconds(timeoutMs);
    }

    waitingClients.push_back(wc);
}

void EventLoop::handleReplicationAck() {
    for (auto& wc : waitingClients) {
        wc.receivedAcks++;
        if (wc.receivedAcks >= wc.needAcks) {
            // Send response
            std::string response = RESPProtocol::encodeInt(wc.receivedAcks);
            scheduleResponse(wc.clientId, response);
        }
    }

    // Remove completed waits
    waitingClients.erase(
        std::remove_if(waitingClients.begin(), waitingClients.end(),
                      [](const WaitingClient& wc) {
                          return wc.receivedAcks >= wc.needAcks;
                      }),
        waitingClients.end()
    );
}

void EventLoop::checkTimeouts() {
    auto now = std::chrono::steady_clock::now();

    // Check blocked clients
    for (auto& [stream, clientList] : blockedClients) {
        clientList.erase(
            std::remove_if(clientList.begin(), clientList.end(),
                          [this, now](int clientId) {
                              auto it = clients.find(clientId);
                              if (it == clients.end()) return true;

                              auto& client = it->second;
                              if (client->blockTimeout != std::chrono::steady_clock::time_point{} &&
                                  now >= client->blockTimeout) {
                                  client->isBlocked = false;
                                  scheduleResponse(clientId, "$-1\r\n"); // Timeout response
                                  return true;
                              }
                              return false;
                          }),
            clientList.end()
        );
    }

    // Check waiting clients
    for (auto& wc : waitingClients) {
        if (wc.timeout != std::chrono::steady_clock::time_point{} &&
            now >= wc.timeout) {
            std::string response = RESPProtocol::encodeInt(wc.receivedAcks);
            scheduleResponse(wc.clientId, response);
        }
    }

    waitingClients.erase(
        std::remove_if(waitingClients.begin(), waitingClients.end(),
                      [now](const WaitingClient& wc) {
                          return wc.timeout != std::chrono::steady_clock::time_point{} &&
                                 now >= wc.timeout;
                      }),
        waitingClients.end()
    );
}

void EventLoop::processPendingResponses() {
    while (!pendingResponses.empty()) {
        auto [clientId, response] = pendingResponses.front();
        pendingResponses.pop();
        scheduleResponse(clientId, response);
    }
}
