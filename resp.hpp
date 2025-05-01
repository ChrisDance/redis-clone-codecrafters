#pragma once

#include <string>
#include <vector>
#include <stdexcept>

class RESPProtocol
{
public:
    static std::string encodeSimpleString(const std::string &s);

    static std::string encodeBulkString(const std::string &s);

    static std::string encodeStringArray(const std::vector<std::string> &arr);

    static std::string encodeArray(const std::vector<std::string> &arr);

    static std::string encodeInt(int n);

    static std::string encodeError(const std::string &e);

    static std::pair<std::vector<std::string>, int> decodeStringArray(const std::string &data);
};