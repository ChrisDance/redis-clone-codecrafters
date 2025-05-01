#pragma once

#include <ctime>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

class RDBFile
{
public:
    static bool readRDB(const std::string &rdbPath,
                        std::unordered_map<std::string, std::string> &store,
                        std::unordered_map<std::string, std::time_t> &ttl);

private:
    static int readEncodedInt(std::ifstream &file);
    static std::string readEncodedString(std::ifstream &file);
    static bool compareBytes(const std::vector<uint8_t> &a, const std::vector<uint8_t> &b);
};