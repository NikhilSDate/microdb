#include <cstdio>
#include <iostream>
#include <print>
#include <db.hpp>

int main() {
    KVStoreConfig config(20, "./db");
    LSMKVStore store(config);
    store.set("hello", "world");
    auto val = store.get("hello");
    std::print("{0}\n", val.value_or("empty"));
    return 0;
}
