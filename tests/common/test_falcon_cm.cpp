#include "test_falcon_cm.h"

zhandle_t *FalconCMIT::zhandle;

FalconCM* FalconCMIT::falconCM;

TEST_F(FalconCMIT, Start)
{
    // already connected
    EXPECT_EQ(falconCM->GetConnState(), ZKConnectionStatus::ZOO_CONNECTED);

    falconCM->TestTriggerWatcher(ZOO_SESSION_EVENT, ZOO_CONNECTED_STATE);

    EXPECT_EQ(falconCM->GetConnState(), ZKConnectionStatus::ZOO_CONNECTED);
}

TEST_F(FalconCMIT, ShouldHandleConnectingState) {
    falconCM->TestTriggerWatcher(ZOO_SESSION_EVENT, ZOO_CONNECTING_STATE);
    EXPECT_EQ(falconCM->GetConnState(), ZKConnectionStatus::ZOO_CONNECTING);
}

TEST_F(FalconCMIT, ExpireBeforeUpLoad) {
    EXPECT_EQ(falconCM->GetNodeState(), StoreNodeStatus::UNINITIALIZED);
    falconCM->TestTriggerWatcher(ZOO_SESSION_EVENT, ZOO_EXPIRED_SESSION_STATE);
    // here reconnect trigered
    // should be fast enough to load local ZOO_EXPIRED_SESSION than other network events
    EXPECT_EQ(falconCM->GetConnState(), ZKConnectionStatus::ZOO_EXPIRED_SESSION);
    sleep(1);
    // should be able to zookeeper_init again
    EXPECT_EQ(falconCM->GetConnState(), ZKConnectionStatus::ZOO_CONNECTED);
    // not initialized, do not ReUpload
    EXPECT_EQ(falconCM->GetNodeState(), StoreNodeStatus::UNINITIALIZED);
}

TEST_F(FalconCMIT, ExpireAfterUpLoad) {
    // upLoad pesdu node info
    std::string podIp = "127.0.0.1";
    std::string rootPath = ".";
    int nodeId = -1;
    int ret = falconCM->Upload("", podIp, nodeId, rootPath);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(falconCM->GetNodeState(), StoreNodeStatus::VALID);

    falconCM->TestTriggerWatcher(ZOO_SESSION_EVENT, ZOO_EXPIRED_SESSION_STATE);
    // here reconnect trigered
    // should be fast enough to load local ZOO_EXPIRED_SESSION than other network events
    EXPECT_EQ(falconCM->GetConnState(), ZKConnectionStatus::ZOO_EXPIRED_SESSION);
    EXPECT_EQ(falconCM->GetNodeState(), StoreNodeStatus::EXPIRED);
    sleep(1);
    // close then init, ephemeral node deleted than created again, with the same name in myid
    EXPECT_EQ(falconCM->GetConnState(), ZKConnectionStatus::ZOO_CONNECTED);
    EXPECT_EQ(falconCM->GetNodeState(), StoreNodeStatus::VALID);

    std::string nodePath = "/falcon/StoreNode/Nodes/Node00" + std::to_string(nodeId);
    int len = 512;
    char buf[len] = {0};
    int getRet = zoo_get(zhandle, nodePath.c_str(), 0, buf, &len, nullptr);
    EXPECT_EQ(getRet, ZOK);
    EXPECT_EQ(strcmp(buf, podIp.c_str()), 0);
}

TEST_F(FalconCMIT, NotConnectedState) {
    // edit exitControl file to not exit the process
    // std::filesystem::remove(FalconCM::GetExitControlFilePath());
    FALCON_LOG(LOG_INFO) << "ExitControlFilePath = " << FalconCM::GetExitControlFilePath();
    if (access(FalconCM::GetExitControlFilePath().c_str(), F_OK) == 0) {
        uint32_t doExit = 0;
        std::ofstream fout;
        fout.open(FalconCM::GetExitControlFilePath().c_str(), std::ios::out);
        if (!fout.is_open()) {
            FALCON_LOG(LOG_ERROR) << "open existed " << FalconCM::GetExitControlFilePath()
                                  << " file failed: " << strerror(errno);
            exit(1);
        }
        fout << doExit;
        fout.close();
    }

    const std::vector<int> failed_states = {
        ZOO_ASSOCIATING_STATE,
        ZOO_AUTH_FAILED_STATE,
        ZOO_NOTCONNECTED_STATE
    };

    for (int state : failed_states) {
        falconCM->ResetState();
        falconCM->TestTriggerWatcher(ZOO_SESSION_EVENT, state);
        // ExitByControlFile(1); called
        EXPECT_EQ(falconCM->GetConnState(), ZKConnectionStatus::ZOO_CONNECTION_FAILED);
    }
}

TEST_F(FalconCMIT, WrongEventType) {
    // non SESSION_EVENT
    falconCM->TestTriggerWatcher(ZOO_CREATED_EVENT, ZOO_CONNECTED_STATE);

    EXPECT_EQ(falconCM->GetConnState(), ZKConnectionStatus::ZOO_CONNECTION_FAILED);
}

TEST_F(FalconCMIT, ShouldHandleUnknownState) {
    // state unknown
    falconCM->ResetState();
    falconCM->TestTriggerWatcher(ZOO_SESSION_EVENT, 9999);

    EXPECT_EQ(falconCM->GetConnState(), ZKConnectionStatus::ZOO_CONNECTION_FAILED);
}

TEST_F(FalconCMIT, RetryWithNumAndInterval) {
    std::function<int()> func0 = []() {
        return RETURN_OK;
    };
    std::function<int()> func1 = []() {
        return RETURN_ERROR;
    };
    bool ret = falconCM->RetryWithNumAndInterval(func0, 2, 1);
    EXPECT_EQ(ret, true);
    ret = falconCM->RetryWithNumAndInterval(func1, 2, 1);
    EXPECT_EQ(ret, false);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
