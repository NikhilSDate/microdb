load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_test", "cc_library")

cc_binary(
    name = "main",
    srcs = [
        "src/db.cpp",
        "src/include/db.hpp",
        "src/include/memtable.hpp",
        "src/include/sstable.hpp",
        "src/include/utils.hpp",
        "src/include/logging.hpp",
        "src/main.cpp",
        "src/memtable.cpp",
        "src/sstable.cpp",
    ],
    includes = ["src/include"],
)

cc_test(
    name = "test",
    srcs = [
        "src/db.cpp",
        "src/include/db.hpp",
        "src/include/memtable.hpp",
        "src/include/sstable.hpp",
        "src/include/utils.hpp",
        "src/memtable.cpp",
        "src/sstable.cpp",
        "src/test.cpp",
    ],
    includes = ["src/include"],
    deps = [
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)
