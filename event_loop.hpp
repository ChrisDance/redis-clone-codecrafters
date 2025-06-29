// event_loop.hpp - Updated for single-threaded operation
#pragma once

#include <sys/event.h>
#include <unordered_map>
#include <memory>
#include <queue>
#include <chrono>
#include <vector>
#include <string>

class ServerState;
class TCPSocket;

struct ClientConnection {
    int fd;
    std::string readBuffer;
    std::string writeBuffer;
    size_t writePos = 0;

    // Transaction state
    bool inMulti = false;
    std::vector<std::vector<std::string>> queuedCommands;

    // Blocking state
    bool isBlocked = false;
    std::string blockedOnStream;
    std::string blockStartId;
    std::chrono::steady_clock::time_point blockTimeout;
};

struct WaitingClient {
    int clientId;
    int needAcks;
    int receivedAcks = 0;
    std::chrono::steady_clock::time_point timeout;
};

class EventLoop {
public:
    EventLoop(ServerState* server);
    ~EventLoop();

    void run();
    void stop();

    void addClient(int fd);
    void removeClient(int fd);
    void scheduleResponse(int clientId, const std::string& response);

    // Master connection management (NEW)
    bool connectToMaster(const std::string& host, int port);
    void disconnectFromMaster();
    bool isMasterConnected() const { return masterFd != -1; }

    // Stream blocking support
    void blockClient(int clientId, const std::string& stream, const std::string& startId, int timeoutMs);
    void notifyStreamClients(const std::string& stream);

    // Replication support
    void addWaitingClient(int clientId, int needAcks, int timeoutMs);
    void handleReplicationAck();

private:
    ServerState* server;
    int kq;
    int serverFd;
    int masterFd = -1;  // NEW: Master connection fd
    bool running = false;

    // Master connection state (NEW)
    std::string masterReadBuffer;
    std::string masterWriteBuffer;
    size_t masterWritePos = 0;
    enum class ReplicationState {
        DISCONNECTED,
        HANDSHAKE_PING,
        HANDSHAKE_REPLCONF1,
        HANDSHAKE_REPLCONF2,
        HANDSHAKE_PSYNC,
        RECEIVING_RDB,
        SYNCHRONIZED
    } replicationState = ReplicationState::DISCONNECTED;
    size_t expectedRdbSize = 0;
    size_t receivedRdbSize = 0;

    std::unordered_map<int, std::unique_ptr<ClientConnection>> clients;
    std::unordered_map<std::string, std::vector<int>> blockedClients;
    std::queue<std::pair<int, std::string>> pendingResponses;
    std::vector<WaitingClient> waitingClients;

    void handleServerRead();
    void handleClientRead(int fd);
    void handleClientWrite(int fd);
    void handleMasterRead();  // NEW
    void handleMasterWrite(); // NEW
    void processCommand(int clientId, const std::vector<std::string>& cmd);
    void checkTimeouts();
    void processPendingResponses();

    // Master connection helpers (NEW)
    void advanceReplicationHandshake();
    void processMasterCommand(const std::vector<std::string>& cmd);
    void sendToMaster(const std::string& data);
};
