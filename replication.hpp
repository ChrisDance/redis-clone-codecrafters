#pragma once

#include <string>
#include <memory>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <atomic>

class TCPSocket;
class ServerState;

struct Replica
{
    std::shared_ptr<TCPSocket> socket;
    int offset;
    int ackOffset;
};

class Replication
{
public:
    explicit Replication(ServerState *server);

    static std::string generateReplicaId();

    bool replicaHandshake();

    static int sendFullResynch(std::shared_ptr<TCPSocket> socket);

    void propagateToReplicas(const std::vector<std::string> &cmd);

    void handlePropagation(std::shared_ptr<TCPSocket> masterSocket);

    std::string handleWait(int count, int timeout);

    void addReplica(std::shared_ptr<TCPSocket> socket);

    size_t getReplicaCount() const;

    void notifyAckReceived();

private:
    ServerState *server;
    std::vector<Replica> replicas;
    mutable std::mutex replicasMutex;
    std::condition_variable ackReceived;
    std::atomic<int> ackCount;
};