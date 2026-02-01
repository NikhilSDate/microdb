load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_test")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

cc_binary(
    name = "main",
    srcs = [
        "src/main.cpp",
        "src/db.cpp",
        "src/memtable.cpp",
        "src/sstable.cpp",
        "src/include/db.hpp",
        "src/include/memtable.hpp",
        "src/include/sstable.hpp",
        "src/include/utils.hpp"
    ],
    includes = ["src/include"],
)

cc_test(
    name = "test",
    srcs = [
        "src/test.cpp",
        "src/sstable.cpp",
        "src/memtable.cpp",
        "src/include/sstable.hpp",
        "src/include/memtable.hpp",
    ],
    includes = ["src/include"],
    deps = ["@catch2//:catch2"]
)