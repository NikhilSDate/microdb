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
        "src/include/wal.hpp",
        "src/include/iterator.hpp",
        "src/include/posix_io.hpp",
        "src/logging.cpp",
        "src/wal.cpp",
        "src/iterator.cpp",
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
        "src/include/logging.hpp",
        "src/include/wal.hpp",
        "src/include/iterator.hpp",
        "src/include/posix_io.hpp",
        "src/logging.cpp",
        "src/wal.cpp",
        "src/iterator.cpp",
        "src/memtable.cpp",
        "src/sstable.cpp",
        "test/test_utils.hpp",
        "test/db_test.cpp",
        "test/block_test.cpp",
        "test/sstable_test.cpp",
        "test/merge_iterator_test.cpp",
    ],
    includes = ["src/include"],
    deps = [
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)
