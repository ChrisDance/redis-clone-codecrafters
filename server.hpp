#pragma once

#include <string>
#include <unordered_map>
#include <memory>
#include <vector>
#include <ctime>

class Stream;
class EventLoop;
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

class ServerState
{
public:
    explicit ServerState(const ServerConfig &config);
    ~ServerState();

    ServerState(const ServerState &) = delete;
    ServerState &operator=(const ServerState &) = delete;

    void start();

    std::string handleCommand(const std::vector<std::string> &cmd, int clientId);

    void incrementReplicaOffset(int amount) { replicaOffset += amount; }

    const ServerConfig &getConfig() const { return config; }
    Replication *getReplication() const { return replication.get(); }
    EventLoop *getEventLoop() const { return eventLoop.get(); }

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

    std::unique_ptr<EventLoop> eventLoop;
    std::unique_ptr<Replication> replication;

    int replicaOffset; // No longer atomic since single-threaded

    bool loadRDBFile();
};
