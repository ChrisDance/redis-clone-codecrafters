#include "resp.hpp"
#include <sstream>
#include <iostream>

std::string RESPProtocol::encodeSimpleString(const std::string &s)
{
    return "+" + s + "\r\n";
}

std::string RESPProtocol::encodeBulkString(const std::string &s)
{
    if (s.empty())
    {
        return "$-1\r\n";
    }
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}

std::string RESPProtocol::encodeStringArray(const std::vector<std::string> &arr)
{
    std::string result = "*" + std::to_string(arr.size()) + "\r\n";
    for (const auto &s : arr)
    {
        result += encodeBulkString(s);
    }
    return result;
}

std::string RESPProtocol::encodeArray(const std::vector<std::string> &arr)
{
    std::string result = "*" + std::to_string(arr.size()) + "\r\n";
    for (const auto &s : arr)
    {
        result += s;
    }
    return result;
}

std::string RESPProtocol::encodeInt(int n)
{
    return ":" + std::to_string(n) + "\r\n";
}

std::string RESPProtocol::encodeError(const std::string &e)
{
    return "-ERR " + e + "\r\n";
}

std::pair<std::vector<std::string>, int> RESPProtocol::decodeStringArray(const std::string &data)
{
    std::vector<std::string> arr;
    int bytesRead = 0;
    int arrSize = 0, strSize = 0;
    std::size_t pos = 0;
    std::string line;

    std::size_t lineEnd = data.find("\r\n", pos);
    if (lineEnd == std::string::npos)
    {
        throw std::runtime_error("Invalid RESP format: missing CRLF");
    }

    line = data.substr(pos, lineEnd - pos);
    pos = lineEnd + 2; /*move past CRLF*/
    bytesRead += line.size() + 2;

    if (line[0] != '*')
    {
        throw std::runtime_error("Invalid RESP format: expected array");
    }

    arrSize = std::stoi(line.substr(1));
    if (arrSize <= 0)
    {
        return {arr, bytesRead};
    }

    for (int i = 0; i < arrSize; i++)
    {

        lineEnd = data.find("\r\n", pos);
        if (lineEnd == std::string::npos)
        {
            throw std::runtime_error("Invalid RESP format: missing CRLF");
        }

        line = data.substr(pos, lineEnd - pos);
        pos = lineEnd + 2; /*move past CRLF*/
        bytesRead += line.size() + 2;

        if (line[0] != '$')
        {
            throw std::runtime_error("Invalid RESP format: expected bulk string");
        }

        strSize = std::stoi(line.substr(1));
        if (strSize < 0)
        {
            arr.push_back("");
            continue;
        }

        if (pos + strSize > data.size())
        {
            throw std::runtime_error("Invalid RESP format: string content exceeds data size");
        }

        std::string str = data.substr(pos, strSize);
        pos += strSize + 2; /* move past CRLF*/
        bytesRead += strSize + 2;
        arr.push_back(str);
    }

    return {arr, bytesRead};
}