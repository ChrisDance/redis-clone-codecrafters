#include "streams.hpp"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <sstream>

Stream::Stream() : first{0, 0}, last{0, 0}
{
}

std::shared_ptr<StreamEntry> Stream::addStreamEntry(const std::string &id)
{
    auto [millisecondsTime, sequenceNumber, success] = getNextID(id);
    if (!success)
    {
        return nullptr;
    }

    if (first[0] == 0 && first[1] == 0)
    {
        first[0] = millisecondsTime;
        first[1] = sequenceNumber;
    }

    last[0] = millisecondsTime;
    last[1] = sequenceNumber;

    auto entry = std::make_shared<StreamEntry>();
    entry->id[0] = millisecondsTime;
    entry->id[1] = sequenceNumber;

    // NO LOCK - single threaded!
    entries.push_back(entry);

    return entry;
}

std::tuple<uint64_t, uint64_t, bool, bool> Stream::splitID(const std::string &id)
{
    uint64_t millisecondsTime = 0;
    uint64_t sequenceNumber = 0;
    bool hasSequence = false;
    bool success = true;

    size_t dashPos = id.find('-');

    try
    {
        if (dashPos != std::string::npos)
        {
            millisecondsTime = std::stoull(id.substr(0, dashPos));
            if (dashPos + 1 < id.size())
            {
                sequenceNumber = std::stoull(id.substr(dashPos + 1));
                hasSequence = true;
            }
        }
        else
        {
            millisecondsTime = std::stoull(id);
        }
    }
    catch (const std::exception &e)
    {
        success = false;
    }

    return {millisecondsTime, sequenceNumber, hasSequence, success};
}

std::tuple<uint64_t, uint64_t, bool> Stream::getNextID(const std::string &id)
{
    uint64_t millisecondsTime = 0;
    uint64_t sequenceNumber = 0;

    size_t dashPos = id.find('-');

    if (id == "*")
    {
        auto now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch();
        millisecondsTime = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

        if (millisecondsTime == last[0])
        {
            sequenceNumber = last[1] + 1;
        }
        else
        {
            sequenceNumber = 0;
        }
    }
    else if (dashPos != std::string::npos && id.substr(dashPos + 1) == "*")
    {
        try
        {
            millisecondsTime = std::stoull(id.substr(0, dashPos));
            if (millisecondsTime == last[0])
            {
                sequenceNumber = last[1] + 1;
            }
            else if (millisecondsTime > last[0])
            {
                sequenceNumber = 0;
            }
            else
            {
                throw std::runtime_error("The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }
        catch (const std::exception &e)
        {
            return {0, 0, false};
        }
    }
    else
    {
        try
        {
            auto [ms, seq, hasSeq, success] = splitID(id);
            if (!success || !hasSeq)
            {
                throw std::runtime_error("Invalid ID format");
            }
            millisecondsTime = ms;
            sequenceNumber = seq;
        }
        catch (const std::exception &e)
        {
            return {0, 0, false};
        }
    }

    if (millisecondsTime == 0 && sequenceNumber == 0)
    {
        throw std::runtime_error("The ID specified in XADD must be greater than 0-0");
    }

    if (millisecondsTime < last[0] || (millisecondsTime == last[0] && sequenceNumber <= last[1]))
    {
        throw std::runtime_error("The ID specified in XADD is equal or smaller than the target stream top item");
    }

    return {millisecondsTime, sequenceNumber, true};
}

int Stream::searchStreamEntries(const std::vector<std::shared_ptr<StreamEntry>> &entries,
                                uint64_t targetMs, uint64_t targetSeq,
                                int lo, int hi)
{
    while (lo <= hi)
    {
        int mid = (lo + hi) / 2;
        auto entry = entries[mid];

        if (targetMs == entry->id[0] && targetSeq == entry->id[1])
        {
            lo = mid;
            break;
        }
        else if (targetMs == entry->id[0] && entry->id[1] > targetSeq)
        {
            hi = mid - 1;
        }
        else if (targetMs == entry->id[0] && entry->id[1] < targetSeq)
        {
            lo = mid + 1;
        }
        else if (targetMs < entry->id[0])
        {
            hi = mid - 1;
        }
        else
        {
            lo = mid + 1;
        }
    }

    return lo;
}
