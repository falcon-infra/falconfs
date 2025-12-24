#include "test_node.h"

#include "connection/node.h"

std::shared_ptr<FalconConfig> NodeUT::config = nullptr;
std::string NodeUT::localEndpoint;
std::vector<std::string> NodeUT::views;

TEST_F(NodeUT, CreateIOConnection)
{
    auto conn = StoreNode::GetInstance()->CreateIOConnection(localEndpoint);
    EXPECT_TRUE(conn);
}

TEST_F(NodeUT, SetNodeConfig)
{
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    std::string clusterView = config->GetArray(FalconPropertyKey::FALCON_CLUSTER_VIEW);
    StoreNode::GetInstance()->SetNodeConfig(nodeId, clusterView);
}

TEST_F(NodeUT, GetNodeId)
{
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    EXPECT_EQ(nodeId, StoreNode::GetInstance()->GetNodeId());
}

TEST_F(NodeUT, GetNodeIdEndpoint)
{
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    auto endpointNodeId = StoreNode::GetInstance()->GetNodeId(localEndpoint);
    EXPECT_EQ(nodeId, endpointNodeId);
}

TEST_F(NodeUT, IsLocalId)
{
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    bool ret = StoreNode::GetInstance()->IsLocal(nodeId);
    EXPECT_TRUE(ret);
    ret = StoreNode::GetInstance()->IsLocal(nodeId + 1);
    EXPECT_FALSE(ret);
}

TEST_F(NodeUT, IsLocalEndpoint)
{
    bool ret = StoreNode::GetInstance()->IsLocal(localEndpoint);
    EXPECT_TRUE(ret);
    ret = StoreNode::GetInstance()->IsLocal("1" + localEndpoint);
    EXPECT_FALSE(ret);
}

TEST_F(NodeUT, GetRpcEndPoint)
{
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    std::string endpoint = StoreNode::GetInstance()->GetRpcEndPoint(nodeId);
    EXPECT_EQ(endpoint, localEndpoint);
}

TEST_F(NodeUT, GetNumberofAllNodes)
{
    int number = StoreNode::GetInstance()->GetNumberofAllNodes();
    EXPECT_EQ(number, views.size());
}

TEST_F(NodeUT, GetAllNodeId)
{
    auto all = StoreNode::GetInstance()->GetAllNodeId();
    std::sort(all.begin(), all.end());
    EXPECT_EQ(all.size(), views.size());
    EXPECT_EQ(all.back(), all.size() - 1);
}

TEST_F(NodeUT, GetRpcConnection)
{
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    auto conn = StoreNode::GetInstance()->GetRpcConnection(nodeId);
    EXPECT_TRUE(conn);
}

TEST_F(NodeUT, AllocNode)
{
    uint64_t inodeId = 100;
    int nodeId = StoreNode::GetInstance()->AllocNode(inodeId);
    auto conn = StoreNode::GetInstance()->GetRpcConnection(nodeId);
    EXPECT_TRUE(conn);
}

TEST_F(NodeUT, GetNextNode)
{
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    auto conn = StoreNode::GetInstance()->GetRpcConnection(nodeId);
    EXPECT_TRUE(conn);

    uint64_t inodeId = 100;
    int nextNodeId = StoreNode::GetInstance()->GetNextNode(nodeId, inodeId);
    EXPECT_NE(nodeId, nextNodeId);
    auto nextConn = StoreNode::GetInstance()->GetRpcConnection(nodeId);
    EXPECT_TRUE(nextConn);
}

TEST_F(NodeUT, UpdateNodeConfigByValueValid)
{
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
    int oldNumber = StoreNode::GetInstance()->GetNumberofAllNodes();
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    StoreNode::GetInstance()->DeleteNode(nodeId);
    int newNumber = StoreNode::GetInstance()->GetNumberofAllNodes();
    EXPECT_EQ(oldNumber, newNumber + 1);
}

TEST_F(NodeUT, Delete)
{
    StoreNode::GetInstance()->Delete();
    int number = StoreNode::GetInstance()->GetNumberofAllNodes();
    EXPECT_EQ(number, 0);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
