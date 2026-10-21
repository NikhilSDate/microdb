#include "iterator.hpp"

MapRangeIterator::MapRangeIterator(std::shared_ptr<const std::map<std::string, std::string>> map,
                                    const std::optional<std::string>& lower)
    : map_(std::move(map)), it_(lower.has_value() ? map_->lower_bound(*lower) : map_->begin()) {}

bool MapRangeIterator::is_valid() const { return it_ != map_->end(); }
const std::string& MapRangeIterator::key() const { return it_->first; }
const std::string& MapRangeIterator::value() const { return it_->second; }
void MapRangeIterator::next() { ++it_; }

bool MergeIterator::Cmp::operator()(size_t a, size_t b) const {
    const auto& ka = (*iters)[a]->key();
    const auto& kb = (*iters)[b]->key();
    if (ka != kb) return ka > kb;
    return a > b;
}

MergeIterator::MergeIterator(std::vector<std::unique_ptr<KVIterator>> iters)
    : iters_(std::move(iters)), heap_(Cmp{&iters_}) {
    for (size_t i = 0; i < iters_.size(); i++) {
        if (iters_[i]->is_valid()) heap_.push(i);
    }
}

bool MergeIterator::is_valid() const { return !heap_.empty(); }
const std::string& MergeIterator::key() const { return iters_[heap_.top()]->key(); }
const std::string& MergeIterator::value() const { return iters_[heap_.top()]->value(); }

void MergeIterator::next() {
    std::string winner_key = key();
    while (!heap_.empty() && iters_[heap_.top()]->key() == winner_key) {
        size_t idx = heap_.top();
        heap_.pop();
        iters_[idx]->next();
        if (iters_[idx]->is_valid()) heap_.push(idx);
    }
}
