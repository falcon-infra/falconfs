#include "test_node.h"

#include "connection/node.h"

std::shared_ptr<FalconConfig> NodeUT::config = nullptr;
std::string NodeUT::localEndpoint;
std::vector<std::string> NodeUT::views;

TEST_F(NodeUT, CreateIOConnection)
{
    /* Exercise Create IO Connection and assert the relevant success or failure branch. */
    auto conn = StoreNode::GetInstance()->CreateIOConnection(localEndpoint);
    EXPECT_TRUE(conn);
}

TEST_F(NodeUT, SetNodeConfig)
{
    /* Exercise Set Node Config and assert the relevant success or failure branch. */
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    std::string clusterView = config->GetArray(FalconPropertyKey::FALCON_CLUSTER_VIEW);
    StoreNode::GetInstance()->SetNodeConfig(nodeId, clusterView);
}

TEST_F(NodeUT, GetNodeId)
{
    /* Exercise Get Node Id and assert the relevant success or failure branch. */
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    EXPECT_EQ(nodeId, StoreNode::GetInstance()->GetNodeId());
    EXPECT_GE(StoreNode::GetInstance()->GetInitStatus(), 0);
}

TEST_F(NodeUT, GetNodeIdEndpoint)
{
    /* Exercise Get Node Id Endpoint and assert the relevant success or failure branch. */
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    auto endpointNodeId = StoreNode::GetInstance()->GetNodeId(localEndpoint);
    EXPECT_EQ(nodeId, endpointNodeId);
}

TEST_F(NodeUT, IsLocalId)
{
    /* Exercise Is local Id and assert the relevant success or failure branch. */
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    bool ret = StoreNode::GetInstance()->IsLocal(nodeId);
    EXPECT_TRUE(ret);
    ret = StoreNode::GetInstance()->IsLocal(nodeId + 1);
    EXPECT_FALSE(ret);
}

TEST_F(NodeUT, IsLocalEndpoint)
{
    /* Exercise Is local Endpoint and assert the relevant success or failure branch. */
    bool ret = StoreNode::GetInstance()->IsLocal(localEndpoint);
    EXPECT_TRUE(ret);
    ret = StoreNode::GetInstance()->IsLocal("1" + localEndpoint);
    EXPECT_FALSE(ret);
}

TEST_F(NodeUT, GetRpcEndPoint)
{
    /* Exercise Get Rpc End Point and assert the relevant success or failure branch. */
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    std::string endpoint = StoreNode::GetInstance()->GetRpcEndPoint(nodeId);
    EXPECT_EQ(endpoint, localEndpoint);
}

TEST_F(NodeUT, GetNumberofAllNodes)
{
    /* Exercise Get Numberof All Nodes and assert the relevant success or failure branch. */
    int number = StoreNode::GetInstance()->GetNumberofAllNodes();
    EXPECT_EQ(number, views.size());
}

TEST_F(NodeUT, GetAllNodeId)
{
    /* Exercise Get All Node Id and assert the relevant success or failure branch. */
    auto all = StoreNode::GetInstance()->GetAllNodeId();
    std::sort(all.begin(), all.end());
    EXPECT_EQ(all.size(), views.size());
    EXPECT_EQ(all.back(), all.size() - 1);
}

TEST_F(NodeUT, GetRpcConnection)
{
    /* Exercise Get Rpc Connection and assert the relevant success or failure branch. */
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    auto conn = StoreNode::GetInstance()->GetRpcConnection(nodeId);
    EXPECT_TRUE(conn);
}

TEST_F(NodeUT, MissingNodeAndInvalidEndpointBranches)
{
    /* Exercise missing Node And invalid Endpoint branches and assert the relevant success or failure branch. */
    int missingNodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID) + 10000;
    EXPECT_EQ(StoreNode::GetInstance()->GetRpcConnection(missingNodeId), nullptr);
    EXPECT_EQ(StoreNode::GetInstance()->GetNodeId("not-an-endpoint"), -1);
    EXPECT_EQ(StoreNode::GetInstance()->GetNodeId("127.0.0.250:59999"), -1);
    EXPECT_FALSE(StoreNode::GetInstance()->IsLocal("not-an-endpoint"));
    EXPECT_EQ(StoreNode::GetInstance()->GetRpcEndPoint(missingNodeId), std::string(""));
}

TEST_F(NodeUT, AllocNode)
{
    /* Exercise Alloc Node and assert the relevant success or failure branch. */
    uint64_t inodeId = 100;
    int nodeId = StoreNode::GetInstance()->AllocNode(inodeId);
    auto conn = StoreNode::GetInstance()->GetRpcConnection(nodeId);
    EXPECT_TRUE(conn);
}

TEST_F(NodeUT, GetNextNode)
{
    /* Exercise Get Next Node and assert the relevant success or failure branch. */
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    auto conn = StoreNode::GetInstance()->GetRpcConnection(nodeId);
    EXPECT_TRUE(conn);

    uint64_t inodeId = 100;
    int nextNodeId = StoreNode::GetInstance()->GetNextNode(nodeId, inodeId);
    EXPECT_NE(nodeId, nextNodeId);
    auto nextConn = StoreNode::GetInstance()->GetRpcConnection(nodeId);
    EXPECT_TRUE(nextConn);
    EXPECT_NE(StoreNode::GetInstance()->GetNextNode(nodeId + 10000, inodeId), -1);
    EXPECT_NE(StoreNode::GetInstance()->GetBackupNodeId(), nodeId);
}

TEST_F(NodeUT, UpdateNodeConfigByValueValid)
{
    /* Exercise Update Node Config By Value Valid and assert the relevant success or failure branch. */
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    std::string endpoint = StoreNode::GetInstance()->GetRpcEndPoint(nodeId);
    EXPECT_EQ(endpoint, localEndpoint);

    std::unordered_map<int, std::string> zkNodes;
    std::string newNode = "localhost:56039";
    zkNodes[nodeId] = newNode;
    zkNodes[nodeId + 10] = newNode;
    zkNodes[nodeId + 100] = newNode;
    StoreNode::GetInstance()->UpdateNodeConfigByValue(zkNodes);
    
    int newNumber = StoreNode::GetInstance()->GetNumberofAllNodes();
    EXPECT_EQ(newNumber, 3);

    endpoint = StoreNode::GetInstance()->GetRpcEndPoint(nodeId + 100);
    EXPECT_EQ(endpoint, newNode);
    endpoint = StoreNode::GetInstance()->GetRpcEndPoint(nodeId + 10);
    EXPECT_EQ(endpoint, newNode);
    endpoint = StoreNode::GetInstance()->GetRpcEndPoint(nodeId);
    EXPECT_EQ(endpoint, newNode);
}

TEST_F(NodeUT, UpdateNodeConfigByValueInvalid)
{
    /* Exercise Update Node Config By Value invalid and assert the relevant success or failure branch. */
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);

    std::unordered_map<int, std::string> zkNodes;
    std::string newNode = "localhost:56039";
    zkNodes[nodeId] = newNode;
    zkNodes[nodeId + 10] = newNode;
    zkNodes[nodeId + 100] = "fakehost:56039";
    StoreNode::GetInstance()->UpdateNodeConfigByValue(zkNodes);
    
    int newNumber = StoreNode::GetInstance()->GetNumberofAllNodes();
    EXPECT_EQ(newNumber, 2);

    std::string endpoint = StoreNode::GetInstance()->GetRpcEndPoint(nodeId + 100);
    EXPECT_EQ(endpoint, std::string(""));
    endpoint = StoreNode::GetInstance()->GetRpcEndPoint(nodeId + 10);
    EXPECT_EQ(endpoint, newNode);
    endpoint = StoreNode::GetInstance()->GetRpcEndPoint(nodeId);
    EXPECT_EQ(endpoint, newNode);
}

TEST_F(NodeUT, DeleteNode)
{
    /* Exercise Delete Node and assert the relevant success or failure branch. */
    int oldNumber = StoreNode::GetInstance()->GetNumberofAllNodes();
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    StoreNode::GetInstance()->DeleteNode(nodeId);
    int newNumber = StoreNode::GetInstance()->GetNumberofAllNodes();
    EXPECT_EQ(oldNumber, newNumber + 1);
}

TEST_F(NodeUT, Delete)
{
    /* Exercise Delete and assert the relevant success or failure branch. */
    StoreNode::GetInstance()->Delete();
    int number = StoreNode::GetInstance()->GetNumberofAllNodes();
    EXPECT_EQ(number, 0);
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    EXPECT_EQ(StoreNode::GetInstance()->AllocNode(100), nodeId);
    EXPECT_EQ(StoreNode::GetInstance()->GetNextNode(nodeId, 100), nodeId);
    EXPECT_FALSE(StoreNode::GetInstance()->IsLocal(localEndpoint));
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
