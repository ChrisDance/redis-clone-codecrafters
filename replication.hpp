#pragma once

#include <string>
#include <memory>
#include <vector>

class TCPSocket;
class ServerState;

struct Replica {
    std::shared_ptr<TCPSocket> socket;
    int offset;
    int ackOffset;
};

class Replication {
public:
    explicit Replication(ServerState* server);

    static std::string generateReplicaId();
    static int sendFullResynch(std::shared_ptr<TCPSocket> socket);

    void propagateToReplicas(const std::vector<std::string>& cmd);
    std::string handleWait(int count, int timeout, int clientId);
    void addReplica(std::shared_ptr<TCPSocket> socket);
    size_t getReplicaCount() const;
    void notifyAckReceived();

    // Removed: replicaHandshake() and handlePropagation()
    // These are now handled by EventLoop

private:
    ServerState* server;
    std::vector<Replica> replicas;
    int ackCount;
};
