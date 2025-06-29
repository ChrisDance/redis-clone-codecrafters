#include "replication.hpp"
#include "tcp_socket.hpp"
#include "event_loop.hpp"
#include "server.hpp"
#include "resp.hpp"
#include <iostream>
#include <sstream>
#include <random>
#include <thread>
#include <algorithm>
#include <chrono>

/* for dependency resolution */
extern const char *EMPTY_RDB_HEX;

Replication::Replication(ServerState *server) : server(server), ackCount(0)
{
}

std::string Replication::generateReplicaId()
{
    const std::string chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(0, chars.size() - 1);

    std::string result(40, ' ');
    for (char &c : result)
    {
        c = chars[dist(gen)];
    }

    return result;
}



/*sends a full RDB dump to a new replica when it connects*/
int Replication::sendFullResynch(std::shared_ptr<TCPSocket> socket)
{
    const char *hexRDB = EMPTY_RDB_HEX;

    std::string binaryRDB;
    size_t len = strlen(hexRDB);
    for (size_t i = 0; i < len; i += 2)
    {
        char byte = (hexRDB[i] >= 'a' ? hexRDB[i] - 'a' + 10 : hexRDB[i] - '0') * 16 +
                    (hexRDB[i + 1] >= 'a' ? hexRDB[i + 1] - 'a' + 10 : hexRDB[i + 1] - '0');
        binaryRDB.push_back(byte);
    }

    std::string sizeHeader = "$" + std::to_string(binaryRDB.size()) + "\r\n";
    /* first size of content, then content */
    socket->send(sizeHeader);
    socket->send(binaryRDB);

    return binaryRDB.size();
}

void Replication::propagateToReplicas(const std::vector<std::string> &cmd)
{

    if (replicas.empty())
    {
        return;
    }

    std::cout << "Propagating command: ";
    for (const auto &s : cmd)
    {
        std::cout << "\"" << s << "\" ";
    }
    std::cout << '\n';

    std::string encodedCmd = RESPProtocol::encodeStringArray(cmd);

    for (auto it = replicas.begin(); it != replicas.end();)
    {
        std::cout << "Replicating to: " << it->socket->getRemoteAddress() << '\n';

        int bytesWritten = it->socket->send(encodedCmd);
        if (bytesWritten <= 0)
        {
            /* can't contact replica, so remove it*/
            std::cout << "Disconnected: " << it->socket->getRemoteAddress() << '\n';
            it = replicas.erase(it);
        }
        else
        {
            it->offset += bytesWritten;
            ++it;
        }
    }
}



std::string Replication::handleWait(int count, int timeout, int clientId)
{
    // Simplified WAIT implementation for single-threaded version
    std::string getAckCmd = RESPProtocol::encodeStringArray({"REPLCONF", "GETACK", "*"});
    int alreadySynced = 0;

    for (auto &replica : replicas) {
        if (replica.offset == 0) {
            alreadySynced++; // No writes to propagate
        } else {
            int bytesWritten = replica.socket->send(getAckCmd);
            if (bytesWritten > 0) {
                replica.offset += bytesWritten;
            } else {
                alreadySynced++; // Count failed replicas as acknowledged
            }
        }
    }

    // Add to event loop's waiting clients
    if (server->getEventLoop() && alreadySynced < count) {
        server->getEventLoop()-> addWaitingClient(clientId, count - alreadySynced, timeout);
        return ""; // Response will be sent later by event loop
    }

    return RESPProtocol::encodeInt(alreadySynced);
}

void Replication::addReplica(std::shared_ptr<TCPSocket> socket)
{
    // NO LOCK - single threaded!
    replicas.push_back({socket, 0, 0});
}

size_t Replication::getReplicaCount() const
{
    // NO LOCK - single threaded!
    return replicas.size();
}

void Replication::notifyAckReceived()
{
    // NO LOCK - single threaded!
    ackCount++;
    // Event loop will handle the actual ACK counting and responses
}
