#pragma once

#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <vector>

class KVIterator {
public:
    virtual ~KVIterator() = default;
    virtual bool is_valid() const = 0;
    virtual const std::string& key() const = 0;
    virtual const std::string& value() const = 0;
    virtual void next() = 0;
};

class MapRangeIterator : public KVIterator {
public:
    MapRangeIterator(std::shared_ptr<const std::map<std::string, std::string>> map,
                      const std::optional<std::string>& lower);

    bool is_valid() const override;
    const std::string& key() const override;
    const std::string& value() const override;
    void next() override;

private:
    std::shared_ptr<const std::map<std::string, std::string>> map_;
    std::map<std::string, std::string>::const_iterator it_;
};

class MergeIterator {
public:
    explicit MergeIterator(std::vector<std::unique_ptr<KVIterator>> iters);

    bool is_valid() const;
    const std::string& key() const;
    const std::string& value() const;
    void next();

private:
    struct Cmp {
        const std::vector<std::unique_ptr<KVIterator>>* iters;
        bool operator()(size_t a, size_t b) const;
    };

    std::vector<std::unique_ptr<KVIterator>> iters_;
    std::priority_queue<size_t, std::vector<size_t>, Cmp> heap_;
};
