#include "replication.hpp"
#include "tcp_socket.hpp"
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

bool Replication::replicaHandshake()
{
    auto &config = server->getConfig();
    auto masterSocket = std::make_shared<TCPSocket>();

    if (!masterSocket->connect(config.replicaofHost, config.replicaofPort))
    {
        std::cerr << "Failed to connect to master" << '\n';
        return false;
    }

    /*handshake*/
    std::vector<std::string> commands[] = {
        {"PING"},
        {"REPLCONF", "listening-port", std::to_string(config.port)},
        {"REPLCONF", "capa", "psync2"},
        {"PSYNC", "?", "-1"}};

    for (const auto &cmd : commands)
    {
        masterSocket->send(RESPProtocol::encodeStringArray(cmd));
        std::string response = masterSocket->readLine();
        if (response.empty())
        {
            std::cerr << "No response from master during handshake" << '\n';
            return false;
        }
        /* TODO: Check responses properly*/
    }

    /*receive RDB file ignoring it for now*/
    std::string response = masterSocket->readLine();
    if (response.empty() || response[0] != '$')
    {
        std::cerr << "Invalid response from master" << '\n';
        return false;
    }

    int rdbSize = std::stoi(response.substr(1));
    std::vector<char> buffer(rdbSize);
    int receivedSize = masterSocket->receive(buffer.data(), rdbSize);

    if (receivedSize != rdbSize)
    {
        std::cerr << "Size mismatch - got: " << receivedSize << ", want: " << rdbSize << '\n';
        return false;
    }

    std::thread propagationThread(&Replication::handlePropagation, this, masterSocket);
    propagationThread.detach();

    return true;
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
    std::lock_guard<std::mutex> lock(replicasMutex);

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

void Replication::handlePropagation(std::shared_ptr<TCPSocket> masterSocket)
{
    while (masterSocket->isValid())
    {

        std::string data = masterSocket->receive();
        if (data.empty())
        {
            break;
        }

        auto [cmd, cmdSize] = RESPProtocol::decodeStringArray(data);
        if (cmd.empty())
        {
            break;
        }

        std::cout << "[from master] Command = ";
        for (const auto &s : cmd)
        {
            std::cout << "\"" << s << "\" ";
        }
        std::cout << '\n';

        auto response = server->handleCommand(cmd, nullptr);

        /* REPLCONF ACK is the only response that a replica sends back to master */
        if (!cmd.empty() && cmd[0] == "REPLCONF")
        {
            masterSocket->send(response);
        }

        server->incrementReplicaOffset(cmdSize);
    }

    std::cout << "Master connection closed" << '\n';
    masterSocket->close();
}

std::string Replication::handleWait(int count, int timeout)
{
    std::vector<std::shared_ptr<TCPSocket>> replicasToNotify;
    int acks = 0;

    {
        std::lock_guard<std::mutex> lock(replicasMutex);

        std::string getAckCmd = RESPProtocol::encodeStringArray({"REPLCONF", "GETACK", "*"});

        for (auto &replica : replicas)
        {
            if (replica.offset > 0)
            {
                int bytesWritten = replica.socket->send(getAckCmd);
                if (bytesWritten > 0)
                {
                    replica.offset += bytesWritten;
                    replicasToNotify.push_back(replica.socket);
                }
                else
                {
                    acks++; /* count failed replicas as acknowledged */
                }
            }
            else
            {
                acks++;
            }
        }
    }

    /* creating a new thread per replica notify is pretty is not great */
    ackCount = 0;
    for (auto &socket : replicasToNotify)
    {
        std::thread([this, socket]()
                    {
            std::cout << "Waiting response from replica " << socket->getRemoteAddress() << '\n';
            std::string response = socket->receive();
            if (!response.empty()) {
                std::cout << "Got response from replica " << socket->getRemoteAddress() << '\n';
            } else {
                std::cout << "Error from replica " << socket->getRemoteAddress() << '\n';
            }
            this->notifyAckReceived(); })
            .detach();
    }

    auto timeoutTime = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout);

    std::unique_lock<std::mutex> lock(replicasMutex);
    while (ackCount + acks < count)
    {
        if (timeout > 0)
        {
            std::cout << "Waiting for acks (timeout = " << timeout << " ms)..." << '\n';
            if (ackReceived.wait_until(lock, timeoutTime) == std::cv_status::timeout)
            {
                std::cout << "Timeout! acks = " << (ackCount + acks) << '\n';
                break;
            }
        }
        else
        {
            std::cout << "Waiting for acks (no timeout)..." << '\n';
            ackReceived.wait(lock);
        }

        std::cout << "Acks = " << (ackCount + acks) << '\n';
    }

    return RESPProtocol::encodeInt(ackCount + acks);
}

void Replication::addReplica(std::shared_ptr<TCPSocket> socket)
{
    std::lock_guard<std::mutex> lock(replicasMutex);
    replicas.push_back({socket, 0, 0});
}

size_t Replication::getReplicaCount() const
{
    std::lock_guard<std::mutex> lock(replicasMutex);
    return replicas.size();
}

void Replication::notifyAckReceived()
{
    std::lock_guard<std::mutex> lock(replicasMutex);
    ackCount++;
    ackReceived.notify_all();
}