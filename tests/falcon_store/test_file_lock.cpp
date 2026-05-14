#include "test_file_lock.h"

#include <future>

FileLock FileLockUT::flk;
uint64_t FileLockUT::id = 0;

TEST_P(FileLockUT, TryLock)
{
    /* Exercise Try Lock and assert the relevant success or failure branch. */
    flk.TryGetFileLock(id, std::get<0>(GetParam()));
    bool succ = flk.TryGetFileLock(id, std::get<1>(GetParam()));
    EXPECT_EQ(succ, std::get<2>(GetParam()));
    flk.ReleaseFileLock(id, std::get<0>(GetParam()));
    if (succ) {
        flk.ReleaseFileLock(id, std::get<1>(GetParam()));
    }
}

TEST_P(FileLockUT, WaitLock)
{
    /* Exercise Wait Lock and assert the relevant success or failure branch. */
    flk.WaitGetFileLock(id, std::get<0>(GetParam()));
    auto fut1 = std::async(std::launch::async, [&]() { flk.WaitGetFileLock(id, std::get<1>(GetParam())); });
    auto status = fut1.wait_for(std::chrono::milliseconds(100));

    EXPECT_EQ(status == std::future_status::ready, std::get<2>(GetParam()));
    flk.ReleaseFileLock(id, std::get<0>(GetParam()));
    fut1.wait();
    flk.ReleaseFileLock(id, std::get<1>(GetParam()));
}

TEST_F(FileLockUT, TestLocked)
{
    /* Exercise Test Locked and assert the relevant success or failure branch. */
    bool succ = flk.TestLocked(id, LockMode::S);
    EXPECT_EQ(succ, false);
    succ = flk.TestLocked(id, LockMode::X);
    EXPECT_EQ(succ, false);

    flk.TryGetFileLock(id, LockMode::S);
    succ = flk.TestLocked(id, LockMode::S);
    EXPECT_EQ(succ, false);
    succ = flk.TestLocked(id, LockMode::X);
    EXPECT_EQ(succ, true);
    flk.ReleaseFileLock(id, LockMode::S);

    flk.TryGetFileLock(id, LockMode::X);
    succ = flk.TestLocked(id, LockMode::S);
    EXPECT_EQ(succ, true);
    succ = flk.TestLocked(id, LockMode::X);
    EXPECT_EQ(succ, true);
    flk.ReleaseFileLock(id, LockMode::X);
}

TEST_F(FileLockUT, ReleaseMissingAndFileLockerBranches)
{
    /* Exercise Release missing And File Locker branches and assert the relevant success or failure branch. */
    uint64_t missingId = id + 1000;
    flk.ReleaseFileLock(missingId, LockMode::S);
    flk.ReleaseFileLock(missingId, LockMode::X);

    {
        FileLocker locker(&flk, missingId, LockMode::S, false);
        EXPECT_TRUE(locker.isLocked());
        EXPECT_FALSE(flk.TestLocked(missingId, LockMode::S));
        EXPECT_TRUE(flk.TestLocked(missingId, LockMode::X));
    }
    EXPECT_FALSE(flk.TestLocked(missingId, LockMode::X));

    ASSERT_TRUE(flk.TryGetFileLock(missingId, LockMode::X));
    {
        FileLocker locker(&flk, missingId, LockMode::S, false);
        EXPECT_FALSE(locker.isLocked());
    }
    flk.ReleaseFileLock(missingId, LockMode::X);
}

INSTANTIATE_TEST_SUITE_P(FileLockSuite,
                         FileLockUT,
                         ::testing::Values(std::make_tuple(LockMode::S, LockMode::S, true),
                                           std::make_tuple(LockMode::S, LockMode::X, false),
                                           std::make_tuple(LockMode::X, LockMode::S, false),
                                           std::make_tuple(LockMode::X, LockMode::X, false)));

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
