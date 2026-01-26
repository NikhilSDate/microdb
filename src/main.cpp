    #include <cstdio>
#include <filesystem>
#include <format>
#include <iostream>
#include <print>
#include <db.hpp>

int main() {
    const std::string db_path = "./test_db";

    // Clean up any existing test database
    if (std::filesystem::exists(db_path)) {
        std::filesystem::remove_all(db_path);
    }

    // Phase 1: Write values to the database
    std::println("=== Phase 1: Writing values ===");
    {
        KVStoreConfig config(10, db_path);
        LSMKVStore db(config);

        for (size_t i = 0; i < 50; i++) {
            auto key = std::format("key{:03d}", i);
            auto value = std::format("value{:03d}", i);
            db.put(key, value);
        }
        std::println("Wrote 50 key-value pairs");
    }
    // db goes out of scope here, simulating "closing" the database

    // Phase 2: Reopen and read values back
    std::println("\n=== Phase 2: Reopening and reading ===");
    {
        KVStoreConfig config(10, db_path);
        LSMKVStore db(config);

        size_t found = 0;
        size_t not_found = 0;
        size_t mismatched = 0;

        for (size_t i = 0; i < 50; i++) {
            auto key = std::format("key{:03d}", i);
            auto expected_value = std::format("value{:03d}", i);
            auto result = db.get(key);

            if (!result.has_value()) {
                std::println("MISS: key '{}' not found", key);
                not_found++;
            } else if (result.value() != expected_value) {
                std::println("MISMATCH: key '{}' expected '{}' got '{}'",
                           key, expected_value, result.value());
                mismatched++;
            } else {
                found++;
            }
        }

        std::println("\nResults: {} found, {} not found, {} mismatched",
                   found, not_found, mismatched);

        if (found == 50 && not_found == 0 && mismatched == 0) {
            std::println("SUCCESS: All values retrieved correctly!");
            return 0;
        } else {
            std::println("FAILURE: Some values were not retrieved correctly");
            return 1;
        }
    }
}
