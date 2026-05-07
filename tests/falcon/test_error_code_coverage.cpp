#include <gtest/gtest.h>

extern "C" {
#include "utils/error_code.h"
}

TEST(FalconErrorCodeCoverageUT, AnalyseNullAndEncodedMessages)
{
    const char *message = reinterpret_cast<const char *>(0x1);
    EXPECT_EQ(FalconErrorMsgAnalyse(nullptr, &message), PROGRAM_ERROR);
    EXPECT_EQ(message, nullptr);

    EXPECT_EQ(FalconErrorMsgAnalyse(nullptr, nullptr), PROGRAM_ERROR);

    const char *encoded = "12345678Aencoded-message";
    EXPECT_EQ(FalconErrorMsgAnalyse(encoded, &message), static_cast<FalconErrorCode>(1));
    EXPECT_STREQ(message, "encoded-message");
}

TEST(FalconErrorCodeCoverageUT, AnalyseUnknownMessages)
{
    const char *message = nullptr;
    const char *plain = "12345678\001plain";
    EXPECT_EQ(FalconErrorMsgAnalyse(plain, &message), UNKNOWN);
    EXPECT_EQ(message, plain);

    const char *outOfRange = "12345678~out-of-range";
    EXPECT_EQ(FalconErrorMsgAnalyse(outOfRange, &message), UNKNOWN);
    EXPECT_EQ(message, outOfRange);

    EXPECT_NE(FalconErrorCodeToString[SUCCESS], nullptr);
    EXPECT_STREQ(FalconErrorCodeToString[SUCCESS], "SUCCESS");
}
