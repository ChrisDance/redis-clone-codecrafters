#include "event_loop.hpp"
#include "server.hpp"
#include "tcp_socket.hpp"
#include "resp.hpp"
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <cstring>
#include <algorithm>

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

bool EventLoop::connectToMaster(const std::string& host, int port) {
    // Create socket
    masterFd = socket(AF_INET, SOCK_STREAM, 0);
    if (masterFd < 0) {
        std::cerr << "Failed to create master socket\n";
        return false;
    }

    // Make non-blocking
    int flags = fcntl(masterFd, F_GETFL, 0);
    fcntl(masterFd, F_SETFL, flags | O_NONBLOCK);

    // Resolve hostname
    struct hostent* he = gethostbyname(host.c_str());
    if (!he) {
        std::cerr << "Could not resolve master hostname: " << host << "\n";
        close(masterFd);
        masterFd = -1;
        return false;
    }

    // Connect
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    std::memcpy(&server_addr.sin_addr, he->h_addr_list[0], he->h_length);

    int result = connect(masterFd, (struct sockaddr*)&server_addr, sizeof(server_addr));
    if (result < 0 && errno != EINPROGRESS) {
        std::cerr << "Failed to connect to master: " << strerror(errno) << "\n";
        close(masterFd);
        masterFd = -1;
        return false;
    }

    // Register with kqueue
    struct kevent ev[2];
    EV_SET(&ev[0], masterFd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
    EV_SET(&ev[1], masterFd, EVFILT_WRITE, EV_ADD, 0, 0, nullptr);
    if (kevent(kq, ev, 2, nullptr, 0, nullptr) < 0) {
        std::cerr << "Failed to register master socket with kqueue\n";
        close(masterFd);
        masterFd = -1;
        return false;
    }

    replicationState = ReplicationState::HANDSHAKE_PING;
    std::cout << "Connecting to master " << host << ":" << port << "\n";

    // Start handshake by sending PING
    sendToMaster(RESPProtocol::encodeStringArray({"PING"}));

    return true;
}

void EventLoop::disconnectFromMaster() {
    if (masterFd != -1) {
        // Remove from kqueue
        struct kevent ev[2];
        EV_SET(&ev[0], masterFd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
        EV_SET(&ev[1], masterFd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        kevent(kq, ev, 2, nullptr, 0, nullptr);

        close(masterFd);
        masterFd = -1;
    }

    replicationState = ReplicationState::DISCONNECTED;
    masterReadBuffer.clear();
    masterWriteBuffer.clear();
    masterWritePos = 0;
}

void EventLoop::sendToMaster(const std::string& data) {
    if (masterFd == -1) return;

    bool wasEmpty = masterWriteBuffer.empty();
    masterWriteBuffer += data;

    // Enable write events if buffer was empty
    if (wasEmpty) {
        struct kevent ev;
        EV_SET(&ev, masterFd, EVFILT_WRITE, EV_ADD, 0, 0, nullptr);
        kevent(kq, &ev, 1, nullptr, 0, nullptr);
    }
}

void EventLoop::run() {
    // Set up server socket
    serverFd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd < 0) {
        throw std::runtime_error("Failed to create server socket");
    }

    int opt = 1;
    if (setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        close(serverFd);
        throw std::runtime_error("Failed to set SO_REUSEADDR");
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(server->getConfig().port);

    if (bind(serverFd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        close(serverFd);
        throw std::runtime_error("Failed to bind server socket");
    }

    if (listen(serverFd, 10) < 0) {
        close(serverFd);
        throw std::runtime_error("Failed to listen on server socket");
    }

    int flags = fcntl(serverFd, F_GETFL, 0);
    fcntl(serverFd, F_SETFL, flags | O_NONBLOCK);

    struct kevent ev;
    EV_SET(&ev, serverFd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
    if (kevent(kq, &ev, 1, nullptr, 0, nullptr) < 0) {
        close(serverFd);
        throw std::runtime_error("Failed to register server socket with kqueue");
    }

    // Connect to master if we're a replica
    if (server->getConfig().role == "slave") {
        if (!connectToMaster(server->getConfig().replicaofHost,
                           server->getConfig().replicaofPort)) {
            std::cerr << "Failed to connect to master, running in disconnected mode\n";
        }
    }

    running = true;
    std::cout << "Single-threaded event loop running on port " << server->getConfig().port << std::endl;

    const int MAX_EVENTS = 64;
    struct kevent events[MAX_EVENTS];

    while (running) {
        struct timespec timeout = {0, 100000000}; // 100ms
        int nev = kevent(kq, nullptr, 0, events, MAX_EVENTS, &timeout);

        if (nev < 0) {
            std::cerr << "kevent error: " << strerror(errno) << std::endl;
            break;
        }

        for (int i = 0; i < nev; i++) {
            const struct kevent& ev = events[i];
            int fd = ev.ident;

            if (fd == serverFd) {
                handleServerRead();
            } else if (fd == masterFd) {
                if (ev.filter == EVFILT_READ) {
                    handleMasterRead();
                } else if (ev.filter == EVFILT_WRITE) {
                    handleMasterWrite();
                }
            } else {
                // Client connection
                if (ev.filter == EVFILT_READ) {
                    handleClientRead(fd);
                } else if (ev.filter == EVFILT_WRITE) {
                    handleClientWrite(fd);
                }
            }
        }

        checkTimeouts();
        processPendingResponses();
    }
}

void EventLoop::stop() {
    running = false;
    disconnectFromMaster();

    if (serverFd != -1) {
        close(serverFd);
        serverFd = -1;
    }

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

void EventLoop::handleMasterRead() {
    if (masterFd == -1) return;

    char buffer[1024];
    ssize_t bytes = recv(masterFd, buffer, sizeof(buffer), 0);

    if (bytes <= 0) {
        if (bytes == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
            std::cerr << "Master connection lost\n";
            disconnectFromMaster();
        }
        return;
    }

    masterReadBuffer.append(buffer, bytes);

    // Process based on replication state
    if (replicationState == ReplicationState::RECEIVING_RDB) {
        receivedRdbSize += bytes;
        if (receivedRdbSize >= expectedRdbSize) {
            std::cout << "RDB transfer complete (" << receivedRdbSize << " bytes)\n";
            replicationState = ReplicationState::SYNCHRONIZED;

            // Clear RDB data from buffer
            size_t rdbDataInBuffer = expectedRdbSize - (receivedRdbSize - bytes);
            masterReadBuffer.erase(0, rdbDataInBuffer);
        }
        return;
    }

    // Try to parse complete responses
    size_t pos = 0;
    while (pos < masterReadBuffer.size()) {
        try {
            if (replicationState >= ReplicationState::SYNCHRONIZED) {
                // Parse commands
                auto [cmd, bytesRead] = RESPProtocol::decodeStringArray(
                    masterReadBuffer.substr(pos));

                if (cmd.empty()) break;

                pos += bytesRead;
                processMasterCommand(cmd);
            } else {
                // Parse handshake responses
                size_t crlfPos = masterReadBuffer.find("\r\n", pos);
                if (crlfPos == std::string::npos) break;

                std::string response = masterReadBuffer.substr(pos, crlfPos - pos);
                pos = crlfPos + 2;

                std::cout << "Master response: " << response << "\n";

                if (response.substr(0, 11) == "FULLRESYNC ") {
                    // Parse RDB size from next line
                    size_t nextCrlf = masterReadBuffer.find("\r\n", pos);
                    if (nextCrlf == std::string::npos) {
                        pos -= response.length() + 2; // Back up
                        break;
                    }

                    std::string sizeLine = masterReadBuffer.substr(pos, nextCrlf - pos);
                    if (sizeLine[0] == '$') {
                        expectedRdbSize = std::stoul(sizeLine.substr(1));
                        receivedRdbSize = 0;
                        replicationState = ReplicationState::RECEIVING_RDB;
                        pos = nextCrlf + 2;
                        std::cout << "Expecting RDB of size " << expectedRdbSize << "\n";
                    }
                } else {
                    advanceReplicationHandshake();
                }
            }
        } catch (const std::exception& e) {
            break; // Incomplete data
        }
    }

    masterReadBuffer.erase(0, pos);
}

void EventLoop::handleMasterWrite() {
    if (masterFd == -1 || masterWriteBuffer.empty()) {
        // Disable write events
        struct kevent ev;
        EV_SET(&ev, masterFd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        kevent(kq, &ev, 1, nullptr, 0, nullptr);
        return;
    }

    ssize_t bytes = send(masterFd, masterWriteBuffer.data() + masterWritePos,
                        masterWriteBuffer.size() - masterWritePos, 0);

    if (bytes < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            std::cerr << "Master write error: " << strerror(errno) << "\n";
            disconnectFromMaster();
        }
        return;
    }

    masterWritePos += bytes;

    if (masterWritePos >= masterWriteBuffer.size()) {
        masterWriteBuffer.clear();
        masterWritePos = 0;

        // Disable write events
        struct kevent ev;
        EV_SET(&ev, masterFd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        kevent(kq, &ev, 1, nullptr, 0, nullptr);
    }
}

void EventLoop::advanceReplicationHandshake() {
    switch (replicationState) {
    case ReplicationState::HANDSHAKE_PING:
        replicationState = ReplicationState::HANDSHAKE_REPLCONF1;
        sendToMaster(RESPProtocol::encodeStringArray({
            "REPLCONF", "listening-port", std::to_string(server->getConfig().port)
        }));
        break;

    case ReplicationState::HANDSHAKE_REPLCONF1:
        replicationState = ReplicationState::HANDSHAKE_REPLCONF2;
        sendToMaster(RESPProtocol::encodeStringArray({
            "REPLCONF", "capa", "psync2"
        }));
        break;

    case ReplicationState::HANDSHAKE_REPLCONF2:
        replicationState = ReplicationState::HANDSHAKE_PSYNC;
        sendToMaster(RESPProtocol::encodeStringArray({
            "PSYNC", "?", "-1"
        }));
        break;

    default:
        break;
    }
}

void EventLoop::processMasterCommand(const std::vector<std::string>& cmd) {
    if (cmd.empty()) return;

    std::cout << "Received from master: ";
    for (const auto& s : cmd) {
        std::cout << "\"" << s << "\" ";
    }
    std::cout << "\n";

    // Process command through server
    std::string response = server->handleCommand(cmd, -1); // Special client ID for master

    // Send ACK for REPLCONF GETACK
    if (cmd.size() >= 2 && cmd[0] == "REPLCONF" && cmd[1] == "GETACK") {
        sendToMaster(response);
    }

    // Update replica offset
    server->incrementReplicaOffset(
        RESPProtocol::encodeStringArray(cmd).length());
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
