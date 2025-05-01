#include "server.hpp"
#include "replication.hpp"
#include <iostream>
#include <string>
#include <cstdlib>
#include <filesystem>

void printUsage()
{
    std::cout << "Usage: redis-clone [options]" << '\n';
    std::cout << "Options:" << '\n';
    std::cout << "  --port <port>           TCP port to listen on (default: 6379)" << '\n';
    std::cout << "  --replicaof <host> <port>  Make this server a replica of the specified master" << '\n';
    std::cout << "  --dir <directory>       Directory for RDB files" << '\n';
    std::cout << "  --dbfilename <filename> Filename for RDB files" << '\n';
    std::cout << "  --help                  Show this help" << '\n';
}

int main(int argc, char *argv[])
{
    ServerConfig config;
    config.port = 6379;
    config.role = "master";
    config.replid = Replication::generateReplicaId();
    config.replOffset = 0;

    for (int i = 1; i < argc; i++)
    {
        std::string arg = argv[i];

        if (arg == "--help")
        {
            printUsage();
            return 0;
        }
        else if (arg == "--port" && i + 1 < argc)
        {
            config.port = std::stoi(argv[++i]);
        }
        else if (arg == "--replicaof" && i + 2 < argc)
        {
            config.role = "slave";
            config.replicaofHost = argv[++i];
            config.replicaofPort = std::stoi(argv[++i]);
        }
        else if (arg == "--dir" && i + 1 < argc)
        {
            config.dir = argv[++i];
        }
        else if (arg == "--dbfilename" && i + 1 < argc)
        {
            config.dbfilename = argv[++i];
        }
        else
        {
            std::cerr << "Unknown option: " << arg << '\n';
            printUsage();
            return 1;
        }
    }

    if (config.role == "master")
    {
        std::cout << "Starting server in master mode" << '\n';
    }
    else
    {
        std::cout << "Starting server in replica mode" << '\n';
        std::cout << "Master: " << config.replicaofHost << ":" << config.replicaofPort << '\n';
    }

    if (!config.dir.empty() && !config.dbfilename.empty())
    {
        if (!std::filesystem::exists(config.dir))
        {
            std::cerr << "Warning: Directory does not exist: " << config.dir << '\n';
            std::cerr << "Creating directory..." << '\n';

            try
            {
                std::filesystem::create_directories(config.dir);
            }
            catch (const std::exception &e)
            {
                std::cerr << "Failed to create directory: " << e.what() << '\n';
                return 1;
            }
        }
    }

    try
    {
        ServerState server(config);
        server.start();
    }
    catch (const std::exception &e)
    {
        std::cerr << "Fatal error: " << e.what() << '\n';
        return 1;
    }

    return 0;
}