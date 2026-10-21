#include <cstdio>

#include <print>
#include <db.hpp>
#include <thread>

int main() {
    KVStoreConfig config(65536, "./test_database");
    LSMKVStore store(config);
    // {
    //     std::jthread t1([&]() {
    //         store.put("abc", "def");
    //     });
    //     std::jthread t2([&]() {
    //         store.put("cde", "fgh");
    //     });
    // }
    auto items = store.scan();
    for (auto [k, v]: items) {
        std::println("key: {}, value: {}", k, v);
    }
    return 0;
}
