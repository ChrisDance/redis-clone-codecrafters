#include "server.hpp"
#include "tcp_socket.hpp"
#include "resp.hpp"
#include "rdb.hpp"
#include "streams.hpp"
#include "replication.hpp"
#include <iostream>
#include <sstream>
#include <thread>
#include <algorithm>
#include <filesystem>
#include <chrono>

/*empty RDB file in hex format for replication*/
const char *EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

ServerState::ServerState(const ServerConfig &config)
    : config(config), replicaOffset(0)
{

    replication = std::make_unique<Replication>(this);
}

ServerState::~ServerState()
{
}

void ServerState::start()
{
    if (config.role == "slave")
    {
        if (!replication->replicaHandshake())
        {
            std::cerr << "Failed to connect to master server" << '\n';
            return;
        }
    }

    if (!config.dir.empty() && !config.dbfilename.empty())
    {
        if (!loadRDBFile())
        {
            std::cerr << "Warning: RDB file loading failed" << '\n';
        }
    }

    server = std::make_unique<TCPServer>();
    if (!server->start(config.port))
    {
        std::cerr << "Failed to bind to port " << config.port << '\n';
        return;
    }

    std::cout << "Listening on port " << config.port << '\n';

    /* --main listen loop--
    Actual redis uses a single threaded event loop with epoll(linux),
    using threads is definitily a worse implementation. */

    int clientId = 1;
    while (server->isRunning())
    {
        auto clientSocket = server->acceptConnection();
        if (clientSocket)
        {
            std::thread(&ServerState::serveClient, this, clientSocket, clientId++).detach();
        }
    }
}

void ServerState::serveClient(std::shared_ptr<TCPSocket> clientSocket, int clientId)
{
    std::cout << "[#" << clientId << "] Client connected: " << clientSocket->getRemoteAddress() << '\n';

    ClientState client(this, clientId, clientSocket);
    client.serve();
}

std::string ServerState::handleCommand(const std::vector<std::string> &cmd, ClientState *client)
{
    if (cmd.empty())
    {
        return "";
    }

    std::string response;
    bool isWrite = false;
    bool resynch = false;

    std::string command = cmd[0];
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);

    if (command == "COMMAND")
    {
        response = RESPProtocol::encodeSimpleString("OK");
    }
    else if (command == "PING")
    {
        response = RESPProtocol::encodeSimpleString("PONG");
    }
    else if (command == "ECHO")
    {
        if (cmd.size() >= 2)
        {
            response = RESPProtocol::encodeBulkString(cmd[1]);
        }
        else
        {
            response = RESPProtocol::encodeError("wrong number of arguments for 'echo' command");
        }
    }
    else if (command == "INFO")
    {
        if (cmd.size() >= 2 && cmd[1] == "REPLICATION")
        {
            std::stringstream info;
            info << "role:" << config.role << "\r\n";
            info << "master_replid:" << config.replid << "\r\n";
            info << "master_repl_offset:" << config.replOffset;
            response = RESPProtocol::encodeBulkString(info.str());
        }
    }
    else if (command == "CONFIG")
    {
        if (cmd.size() >= 3 && cmd[1] == "GET")
        {
            if (cmd[2] == "dir")
            {
                response = RESPProtocol::encodeStringArray({"dir", config.dir});
            }
            else if (cmd[2] == "dbfilename")
            {
                response = RESPProtocol::encodeStringArray({"dbfilename", config.dbfilename});
            }
        }
    }
    else if (command == "SET")
    {
        isWrite = true;
        if (cmd.size() >= 3)
        {
            const std::string &key = cmd[1];
            const std::string &value = cmd[2];
            store[key] = value;

            if (cmd.size() >= 5 && cmd[3] == "PX")
            {
                int expiration = std::stoi(cmd[4]);
                auto now = std::chrono::system_clock::now();
                auto expirationTime = now + std::chrono::milliseconds(expiration);
                ttl[key] = std::chrono::system_clock::to_time_t(expirationTime);
            }

            response = RESPProtocol::encodeSimpleString("OK");
        }
        else
        {
            response = RESPProtocol::encodeError("wrong number of arguments for 'set' command");
        }
    }
    else if (command == "GET")
    {
        if (cmd.size() >= 2)
        {
            const std::string &key = cmd[1];
            auto it = store.find(key);
            if (it != store.end())
            {
                auto ttlIt = ttl.find(key);

                auto now = std::time(nullptr);
                if (ttlIt == ttl.end() || ttlIt->second > now)
                {
                    response = RESPProtocol::encodeBulkString(it->second);
                }
                else
                {

                    ttl.erase(ttlIt);
                    store.erase(it);
                    response = RESPProtocol::encodeBulkString("");
                }
            }
            else
            {
                response = RESPProtocol::encodeBulkString("");
            }
        }
        else
        {
            response = RESPProtocol::encodeError("wrong number of arguments for 'get' command");
        }
    }
    else if (command == "INCR")
    {
        isWrite = true;
        if (cmd.size() >= 2)
        {
            const std::string &key = cmd[1];
            auto it = store.find(key);

            if (it != store.end())
            {
                try
                {
                    int value = std::stoi(it->second);
                    value++;
                    store[key] = std::to_string(value);
                    response = RESPProtocol::encodeInt(value);
                }
                catch (const std::exception &e)
                {
                    response = RESPProtocol::encodeError("value is not an integer or out of range");
                }
            }
            else
            {
                /* key doesn't exist, initialise as 1 */
                store[key] = "1";
                response = RESPProtocol::encodeInt(1);
            }
        }
        else
        {
            response = RESPProtocol::encodeError("wrong number of arguments for 'incr' command");
        }
    }
    else if (command == "MULTI") /* begin transaction */
    {
        if (client)
        {
            client->getMulti() = true;
            response = RESPProtocol::encodeSimpleString("OK");
        }
    }
    else if (command == "REPLCONF")
    {
        if (cmd.size() >= 2)
        {
            std::string subcommand = cmd[1];
            std::transform(subcommand.begin(), subcommand.end(), subcommand.begin(), ::toupper);

            if (subcommand == "GETACK")
            {
                response = RESPProtocol::encodeStringArray({"REPLCONF", "ACK", std::to_string(replicaOffset)});
            }
            else if (subcommand == "ACK")
            {
                replication->notifyAckReceived();
                response = "";
            }
            else
            {
                /* other REPLCONF commands - just ack*/
                response = RESPProtocol::encodeSimpleString("OK");
            }
        }
    }
    else if (command == "PSYNC")
    {
        if (cmd.size() >= 3)
        {
            response = RESPProtocol::encodeSimpleString("FULLRESYNC " + config.replid + " 0");
            resynch = true;
        }
    }
    else if (command == "WAIT")
    {
        if (cmd.size() >= 3)
        {
            int count = std::stoi(cmd[1]);
            int timeout = std::stoi(cmd[2]);
            response = replication->handleWait(count, timeout);
        }
    }
    else if (command == "KEYS")
    {
        std::vector<std::string> keys;
        keys.reserve(store.size());

        for (const auto &[key, _] : store)
        {
            keys.push_back(key);
        }

        response = RESPProtocol::encodeStringArray(keys);
    }
    else if (command == "TYPE")
    {
        if (cmd.size() >= 2)
        {
            const std::string &key = cmd[1];

            if (streams.find(key) != streams.end())
            {
                response = RESPProtocol::encodeSimpleString("stream");
            }
            else if (store.find(key) != store.end())
            {
                response = RESPProtocol::encodeSimpleString("string");
            }
            else
            {
                response = RESPProtocol::encodeSimpleString("none");
            }
        }
    }
    else if (command == "XADD")
    {
        if (cmd.size() >= 4)
        {
            const std::string &streamKey = cmd[1];
            const std::string &id = cmd[2];

            std::vector<std::string> pairs;
            for (size_t i = 3; i < cmd.size(); i++)
            {
                pairs.push_back(cmd[i]);
            }

            response = handleStreamAdd(streamKey, id, pairs);
            isWrite = true;
        }
    }
    else if (command == "XRANGE")
    {
        if (cmd.size() >= 4)
        {
            const std::string &streamKey = cmd[1];
            const std::string &start = cmd[2];
            const std::string &end = cmd[3];

            response = handleStreamRange(streamKey, start, end);
        }
    }
    else if (command == "XREAD")
    {
        response = handleStreamRead(cmd);
    }
    else
    {
        response = RESPProtocol::encodeError("unknown command '" + command + "'");
    }

    if (isWrite)
    {
        replication->propagateToReplicas(cmd);
    }

    if (client && resynch)
    {
        int size = Replication::sendFullResynch(client->getSocket());
        std::cout << "[#" << client->getId() << "] full resynch sent: " << size << '\n';
        replication->addReplica(client->getSocket());
        std::cout << "[#" << client->getId() << "] Client promoted to replica" << '\n';
    }

    return response;
}

bool ServerState::loadRDBFile()
{
    std::string rdbPath = std::filesystem::path(config.dir) / config.dbfilename;
    return RDBFile::readRDB(rdbPath, store, ttl);
}

std::string ServerState::handleStreamAdd(const std::string &streamKey, const std::string &id,
                                         const std::vector<std::string> &pairs)
{
    auto streamIt = streams.find(streamKey);
    if (streamIt == streams.end())
    {
        streams[streamKey] = std::make_unique<Stream>();
        streamIt = streams.find(streamKey);
    }

    auto entry = streamIt->second->addStreamEntry(id);
    if (!entry)
    {
        return RESPProtocol::encodeError("Invalid stream ID format");
    }

    for (size_t i = 0; i < pairs.size(); i += 2)
    {
        if (i + 1 < pairs.size())
        {
            entry->store.push_back(pairs[i]);
            entry->store.push_back(pairs[i + 1]);
        }
    }

    streamIt->second->notifyBlocked();

    return RESPProtocol::encodeBulkString(
        std::to_string(entry->id[0]) + "-" + std::to_string(entry->id[1]));
}

std::string ServerState::handleStreamRange(const std::string &streamKey, const std::string &start,
                                           const std::string &end)
{
    auto streamIt = streams.find(streamKey);
    if (streamIt == streams.end() || streamIt->second->getEntries().empty())
    {
        return "*0\r\n";
    }

    const auto &entries = streamIt->second->getEntries();
    int startIndex = 0, endIndex = 0;

    if (start == "-")
    {
        startIndex = 0;
    }
    else
    {
        auto [startMs, startSeq, startHasSeq, _] = streamIt->second->splitID(start);
        if (!startHasSeq)
        {
            startSeq = 0;
        }
        startIndex = Stream::searchStreamEntries(entries, startMs, startSeq, 0, entries.size() - 1);
    }

    if (end == "+")
    {
        endIndex = entries.size() - 1;
    }
    else
    {
        auto [endMs, endSeq, endHasSeq, _] = streamIt->second->splitID(end);
        if (!endHasSeq)
        {
            endSeq = std::numeric_limits<uint64_t>::max();
        }
        endIndex = Stream::searchStreamEntries(entries, endMs, endSeq, startIndex, entries.size() - 1);
        if (endIndex >= entries.size())
        {
            endIndex = entries.size() - 1;
        }
    }

    /*build response*/
    int entriesCount = endIndex - startIndex + 1;
    std::string response = "*" + std::to_string(entriesCount) + "\r\n";

    for (int index = startIndex; index <= endIndex; index++)
    {
        const auto &entry = entries[index];
        std::string id = std::to_string(entry->id[0]) + "-" + std::to_string(entry->id[1]);

        response += "*2\r\n$" + std::to_string(id.length()) + "\r\n" + id + "\r\n";
        response += "*" + std::to_string(entry->store.size()) + "\r\n";

        for (const auto &kv : entry->store)
        {
            response += RESPProtocol::encodeBulkString(kv);
        }
    }

    return response;
}

std::string ServerState::handleStreamRead(const std::vector<std::string> &cmd)
{

    bool isBlocking = false;
    int blockTimeout = 0;
    int readKeyIndex = 2;

    if (cmd.size() > 1 && cmd[1] == "BLOCK")
    {
        isBlocking = true;
        blockTimeout = std::stoi(cmd[2]);
        readKeyIndex += 2;
    }

    int readCount = (cmd.size() - readKeyIndex) / 2;
    int readStartIndex = readKeyIndex + readCount;

    std::vector<std::pair<std::string, std::string>> readParams;

    for (int i = 0; i < readCount; i++)
    {
        const std::string &streamKey = cmd[i + readKeyIndex];
        const std::string &start = cmd[i + readStartIndex];

        if (streams.find(streamKey) != streams.end())
        {
            readParams.emplace_back(streamKey, start);
        }
    }

    std::string response = "*" + std::to_string(readParams.size()) + "\r\n";

    for (const auto &[streamKey, start] : readParams)
    {

        response += "*2\r\n";
        response += RESPProtocol::encodeBulkString(streamKey);

        auto &stream = streams[streamKey];

        uint64_t startMs, startSeq;
        bool startHasSeq;

        if (start == "$")
        {
            startMs = stream->getLast()[0];
            startSeq = stream->getLast()[1];
        }
        else
        {
            auto [ms, seq, hasSeq, _] = stream->splitID(start);
            startMs = ms;
            startSeq = seq;
            startHasSeq = hasSeq;

            if (!startHasSeq)
            {
                startSeq = 0;
            }
        }

        const auto &entries = stream->getEntries();
        std::shared_ptr<StreamEntry> entry;
        int startIndex = 0;

        while (!entry)
        {
            startIndex = Stream::searchStreamEntries(entries, startMs, startSeq, startIndex, entries.size() - 1);

            if (startIndex < entries.size())
            {
                entry = entries[startIndex];
            }

            /*if found exact match, need to get the next one as xread bound is exclusive */
            if (entry && entry->id[0] == startMs && entry->id[1] == startSeq)
            {
                if (startIndex + 1 < entries.size())
                {
                    entry = entries[startIndex + 1];
                }
                else
                {
                    entry = nullptr;
                }
            }

            if (!entry && isBlocking)
            {

                std::mutex mtx;
                std::unique_lock<std::mutex> lock(mtx);
                std::condition_variable cv;

                stream->blockClient(&cv);

                bool timedOut = false;
                if (blockTimeout > 0)
                {
                    std::cout << "Waiting for a write on stream " << streamKey
                              << " (timeout = " << blockTimeout << " ms)..." << '\n';

                    auto result = cv.wait_for(lock, std::chrono::milliseconds(blockTimeout));
                    timedOut = (result == std::cv_status::timeout);
                }
                else
                {
                    std::cout << "Waiting for a write on stream " << streamKey
                              << " (no timeout!)..." << '\n';

                    cv.wait(lock);
                }

                stream->unblockClient(&cv);

                if (timedOut)
                {
                    return "$-1\r\n";
                }
            }
            else
            {
                break;
            }
        }

        if (!entry)
        {
            return "*0\r\n";
        }

        response += "*1\r\n";
        std::string id = std::to_string(entry->id[0]) + "-" + std::to_string(entry->id[1]);
        response += "*2\r\n$" + std::to_string(id.length()) + "\r\n" + id + "\r\n";
        response += "*" + std::to_string(entry->store.size()) + "\r\n";

        for (const auto &kv : entry->store)
        {
            response += RESPProtocol::encodeBulkString(kv);
        }
    }

    return response;
}

ClientState::ClientState(ServerState *server, int id, std::shared_ptr<TCPSocket> socket)
    : server(server), id(id), socket(socket), multi(false)
{
}

void ClientState::serve()
{
    std::string buffer;

    while (socket->isValid())
    {
        std::string data = socket->receive();
        if (data.empty())
        {
            break;
        }

        buffer += data;

        try
        {
            auto [cmd, bytesRead] = RESPProtocol::decodeStringArray(buffer);
            buffer.erase(0, bytesRead);

            if (cmd.empty())
            {
                break;
            }

            std::string response;
            bool resynch = false;

            if (cmd[0] == "EXEC") /* all commands since multi was called*/
            {
                if (getMulti())
                {
                    std::vector<std::string> responses;

                    for (const auto &queuedCmd : getQueue())
                    {
                        std::string cmdResponse = server->handleCommand(queuedCmd, this);
                        responses.push_back(cmdResponse);
                    }

                    response = RESPProtocol::encodeArray(responses);
                    getQueue().clear();
                    getMulti() = false;
                }
                else
                {
                    response = RESPProtocol::encodeError("EXEC without MULTI");
                }
            }
            else if (cmd[0] == "DISCARD")
            {
                if (getMulti())
                {
                    response = RESPProtocol::encodeSimpleString("OK");
                    getQueue().clear();
                    getMulti() = false;
                }
                else
                {
                    response = RESPProtocol::encodeError("DISCARD without MULTI");
                }
            }
            else if (getMulti())
            {
                getQueue().push_back(cmd);
                response = RESPProtocol::encodeSimpleString("QUEUED");
            }
            else
            {
                std::cout << "[#" << id << "] Command = ";
                for (const auto &s : cmd)
                {
                    std::cout << "\"" << s << "\" ";
                }
                std::cout << '\n';

                response = server->handleCommand(cmd, this);
            }

            if (!response.empty())
            {
                int bytesSent = socket->send(response);
                if (bytesSent <= 0)
                {
                    std::cout << "[#" << id << "] Error writing response" << '\n';
                    break;
                }

                std::cout << "[#" << id << "] Bytes sent: " << bytesSent << " \""
                          << (response.length() > 100 ? response.substr(0, 100) + "..." : response)
                          << "\"" << '\n';
            }
        }
        catch (const std::exception &e)
        {
            std::cout << "[#" << id << "] Error: " << e.what() << '\n';
            break;
        }
    }

    std::cout << "[#" << id << "] Client closing" << '\n';
    socket->close();
}
