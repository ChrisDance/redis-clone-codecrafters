#pragma once

#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <tuple>
#include <vector>
#include <limits>

struct StreamEntry
{
    std::array<uint64_t, 2> id;
    std::vector<std::string> store;
};

class Stream
{
public:
    Stream();

    std::shared_ptr<StreamEntry> addStreamEntry(const std::string &id);

    std::tuple<uint64_t, uint64_t, bool, bool> splitID(const std::string &id);

    std::tuple<uint64_t, uint64_t, bool> getNextID(const std::string &id);

    static int searchStreamEntries(const std::vector<std::shared_ptr<StreamEntry>> &entries,
                                   uint64_t targetMs, uint64_t targetSeq,
                                   int lo, int hi);

    const std::vector<std::shared_ptr<StreamEntry>> &getEntries() const { return entries; }

    const std::array<uint64_t, 2> &getFirst() const { return first; }
    const std::array<uint64_t, 2> &getLast() const { return last; }

private:
    std::array<uint64_t, 2> first;
    std::array<uint64_t, 2> last;
    std::vector<std::shared_ptr<StreamEntry>> entries;
};
