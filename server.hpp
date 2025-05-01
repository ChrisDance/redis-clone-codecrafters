#pragma once

#include <string>
#include <unordered_map>
#include <memory>
#include <vector>
#include <ctime>
#include <atomic>
#include <thread>
#include <functional>

class Stream;
class TCPSocket;
class TCPServer;
class Replication;

struct ServerConfig
{
    int port;
    std::string role;
    std::string replid;
    int replOffset;
    std::string replicaofHost;
    int replicaofPort;
    std::string dir;
    std::string dbfilename;
};

class ClientState;

class ServerState
{
public:
    explicit ServerState(const ServerConfig &config);
    ~ServerState();

    ServerState(const ServerState &) = delete;
    ServerState &operator=(const ServerState &) = delete;

    void start();

    std::string handleCommand(const std::vector<std::string> &cmd, ClientState *client);

    void incrementReplicaOffset(int amount) { replicaOffset += amount; }

    const ServerConfig &getConfig() const { return config; }
    Replication *getReplication() const { return replication.get(); }

    std::string handleStreamAdd(const std::string &streamKey, const std::string &id,
                                const std::vector<std::string> &pairs);
    std::string handleStreamRange(const std::string &streamKey, const std::string &start,
                                  const std::string &end);
    std::string handleStreamRead(const std::vector<std::string> &cmd);

private:
    ServerConfig config;

    std::unordered_map<std::string, std::string> store;
    std::unordered_map<std::string, std::time_t> ttl;
    std::unordered_map<std::string, std::unique_ptr<Stream>> streams;

    std::unique_ptr<TCPServer> server;
    std::unique_ptr<Replication> replication;

    std::atomic<int> replicaOffset;

    void serveClient(std::shared_ptr<TCPSocket> clientSocket, int clientId);

    bool loadRDBFile();
};

class ClientState
{
public:
    ClientState(ServerState *server, int id, std::shared_ptr<TCPSocket> socket);

    ClientState(const ClientState &) = delete;
    ClientState &operator=(const ClientState &) = delete;

    void serve();

    int getId() const { return id; }
    std::shared_ptr<TCPSocket> getSocket() const { return socket; }
    ServerState *getServer() const { return server; }
    bool &getMulti() { return multi; }
    std::vector<std::vector<std::string>> &getQueue() { return queue; }

private:
    ServerState *server;
    int id;
    std::shared_ptr<TCPSocket> socket;
    bool multi;
    std::vector<std::vector<std::string>> queue;
};