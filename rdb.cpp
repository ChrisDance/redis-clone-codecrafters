#include "rdb.hpp"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <stdexcept>

/* stores key-values with ttl info in binary format requiring ugly casting */

bool RDBFile::compareBytes(const std::vector<uint8_t> &a, const std::vector<uint8_t> &b)
{
    if (a.size() != b.size())
        return false;
    return std::equal(a.begin(), a.end(), b.begin());
}

int RDBFile::readEncodedInt(std::ifstream &file)
{
    uint8_t b0;
    file.read(reinterpret_cast<char *>(&b0), 1);
    if (!file)
        throw std::runtime_error("Failed to read encoded int");

    uint8_t mask = 0b11000000;
    if ((b0 & mask) == 0b00000000)
    {
        return static_cast<int>(b0);
    }
    else if ((b0 & mask) == 0b01000000)
    {
        uint8_t b1;
        file.read(reinterpret_cast<char *>(&b1), 1);
        if (!file)
            throw std::runtime_error("Failed to read encoded int");
        return (static_cast<int>(b1) << 6) | static_cast<int>(b0 & mask);
    }
    else if ((b0 & mask) == 0b10000000)
    {
        uint8_t b1, b2, b3, b4;
        file.read(reinterpret_cast<char *>(&b1), 1);
        file.read(reinterpret_cast<char *>(&b2), 1);
        file.read(reinterpret_cast<char *>(&b3), 1);
        file.read(reinterpret_cast<char *>(&b4), 1);
        if (!file)
            throw std::runtime_error("Failed to read encoded int");
        return (static_cast<int>(b1) << 24) |
               (static_cast<int>(b2) << 16) |
               (static_cast<int>(b3) << 8) |
               static_cast<int>(b4);
    }
    else if (b0 >= 0b11000000 && b0 <= 0b11000010)
    { /* special format: integers as string */
        uint8_t b1 = 0, b2 = 0, b3 = 0, b4 = 0;
        file.read(reinterpret_cast<char *>(&b1), 1);
        if (b0 >= 0b11000001)
        {
            file.read(reinterpret_cast<char *>(&b2), 1);
        }
        if (b0 == 0b11000010)
        {
            file.read(reinterpret_cast<char *>(&b3), 1);
            file.read(reinterpret_cast<char *>(&b4), 1);
        }
        if (!file)
            throw std::runtime_error("Failed to read encoded int");
        return static_cast<int>(b1) |
               (static_cast<int>(b2) << 8) |
               (static_cast<int>(b3) << 16) |
               (static_cast<int>(b4) << 24);
    }
    else
    {
        throw std::runtime_error("Not implemented encoding format");
    }
}

std::string RDBFile::readEncodedString(std::ifstream &file)
{
    int size = readEncodedInt(file);
    std::vector<char> data(size);
    file.read(data.data(), size);
    if (!file || file.gcount() != size)
    {
        throw std::runtime_error("Unexpected string length");
    }
    return std::string(data.data(), size);
}

bool RDBFile::readRDB(const std::string &rdbPath,
                      std::unordered_map<std::string, std::string> &store,
                      std::unordered_map<std::string, std::time_t> &ttl)
{
    std::ifstream file(rdbPath, std::ios::binary);
    if (!file.is_open())
    {
        std::cerr << "Failed to open RDB file: " << rdbPath << '\n';
        return false;
    }

    std::vector<uint8_t> header(9);
    file.read(reinterpret_cast<char *>(header.data()), 9);

    std::vector<uint8_t> redisHeader = {'R', 'E', 'D', 'I', 'S'};
    if (!compareBytes(std::vector<uint8_t>(header.begin(), header.begin() + 5), redisHeader))
    {
        std::cerr << "Not a redis file mate" << '\n';
        return false;
    }

    std::string versionStr(header.begin() + 5, header.end());
    int version = std::stoi(versionStr);
    std::cout << "File version: " << version << '\n';

    bool eof = false;
    while (!eof)
    {
        bool startDataRead = false;
        uint8_t opCode;

        file.read(reinterpret_cast<char *>(&opCode), 1);
        if (!file)
        {
            if (file.eof())
            {
                break;
            }
            std::cerr << "Failed to read opcode" << '\n';
            return false;
        }

        switch (opCode)
        {
        /** data is preceeded by opecode explaining how it should be parsed */
        case 0xFA: /* Auxillary fields*/
        {
            std::string key = readEncodedString(file);
            if (key == "redis-ver")
            {
                std::string value = readEncodedString(file);
                std::cout << "Aux: " << key << " = " << value << '\n';
            }
            else if (key == "redis-bits")
            {
                int bits = readEncodedInt(file);
                std::cout << "Aux: " << key << " = " << bits << '\n';
            }
            else if (key == "ctime")
            {
                int ctime = readEncodedInt(file);
                std::cout << "Aux: " << key << " = " << ctime << " ("
                          << std::ctime(reinterpret_cast<const std::time_t *>(&ctime)) << ")" << '\n';
            }
            else if (key == "used-mem")
            {
                int usedmem = readEncodedInt(file);
                std::cout << "Aux: " << key << " = " << usedmem << '\n';
            }
            else if (key == "aof-preamble")
            {
                int size = readEncodedInt(file);
                std::cout << "Aux: " << key << " = " << size << '\n';
            }
            else
            {
                std::cerr << "Unknown auxiliary field: " << key << '\n';
                return false;
            }
            break;
        }
        case 0xFB: /*hash table sizes for the main keyspace and expires */
        {
            int keyspace = readEncodedInt(file);
            int expires = readEncodedInt(file);
            std::cout << "Hash table sizes: keyspace = " << keyspace << ", expires = " << expires << '\n';
            startDataRead = true;
            break;
        }
        case 0xFE: /* DB selector*/
        {
            int db = readEncodedInt(file);
            std::cout << "Database Selector = " << db << '\n';
            break;
        }
        case 0xFF: /* end */
            eof = true;
            break;
        default:
            std::cerr << "Unknown op code: " << std::hex << static_cast<int>(opCode) << std::dec << '\n';
            return false;
        }

        if (startDataRead)
        {
            while (true)
            {
                uint8_t valueType;
                file.read(reinterpret_cast<char *>(&valueType), 1);
                if (!file)
                {
                    return false;
                }

                std::time_t expiration = 0;
                if (valueType == 0xFD)
                {
                    std::vector<uint8_t> bytes(4);
                    file.read(reinterpret_cast<char *>(bytes.data()), 4);
                    expiration = static_cast<std::time_t>(bytes[0] |
                                                          (bytes[1] << 8) |
                                                          (bytes[2] << 16) |
                                                          (bytes[3] << 24));
                    file.read(reinterpret_cast<char *>(&valueType), 1);
                }
                else if (valueType == 0xFC)
                {
                    std::vector<uint8_t> bytes(8);
                    file.read(reinterpret_cast<char *>(bytes.data()), 8);
                    int64_t millisTime = static_cast<int64_t>(bytes[0]) |
                                         (static_cast<int64_t>(bytes[1]) << 8) |
                                         (static_cast<int64_t>(bytes[2]) << 16) |
                                         (static_cast<int64_t>(bytes[3]) << 24) |
                                         (static_cast<int64_t>(bytes[4]) << 32) |
                                         (static_cast<int64_t>(bytes[5]) << 40) |
                                         (static_cast<int64_t>(bytes[6]) << 48) |
                                         (static_cast<int64_t>(bytes[7]) << 56);
                    expiration = static_cast<std::time_t>(millisTime / 1000);
                    file.read(reinterpret_cast<char *>(&valueType), 1);
                }
                else if (valueType == 0xFF)
                {
                    startDataRead = false;
                    file.unget();
                    break;
                }

                if (!file)
                {
                    return false;
                }

                if (valueType != 0)
                {
                    std::cerr << "Value type not implemented: " << std::hex << static_cast<int>(valueType) << std::dec << '\n';
                    return false;
                }

                std::string key = readEncodedString(file);
                std::string value = readEncodedString(file);
                std::cout << "Reading key/value: \"" << key << "\" => \"" << value << "\" Expiration: ("
                          << (expiration ? std::ctime(&expiration) : "none") << ")" << '\n';

                std::time_t now = std::time(nullptr);

                if (expiration == 0 || expiration > now)
                {
                    if (expiration > now)
                    {
                        ttl[key] = expiration;
                    }
                    store[key] = value;
                }
            }
        }
    }

    return true;
}