cc_binary(
    name = "main",
    srcs = [
        "src/main.cpp",
        "src/db.cpp",
        "src/sstable.cpp",
        "src/include/db.hpp",
        "src/include/sstable.hpp",
    ],
    includes = ["src/include"],
)

cc_test(
    name = "sstable_test",
    srcs = [
        "src/sstable_test.cpp",
        "src/sstable.cpp",
        "src/include/sstable.hpp",
    ],
    includes = ["src/include"],
    deps = [
        "@catch2//:catch2_main",
    ],
)