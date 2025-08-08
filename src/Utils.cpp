//
// Created by Manh Nguyen Viet on 7/21/25.
//
#include <bitset>

#include "Utils.h"
#include "all.hpp"

std::string EncodeArr2RespArr(std::vector<std::string> &arr) {
    std::string resp_str;
    resp::encoder<std::string> enc;
    auto encoded_arr = enc.encode_arr(arr);
    for (auto &encoded: encoded_arr) {
        resp_str += encoded;
    }
    return resp_str;
}

std::string EncodeRespSimpleStr(std::string s) {
    resp::encoder<std::string> enc;
    std::string resp_str = enc.encode_simple_str(s);
    return resp_str;
}

void ResetQuery(Query &query) {
    query.cmd_type = UnknownCmd;
    query.cmd_args.clear();
}

int RdbStat(const std::string &file_name, struct stat &st) {
    if (stat(file_name.c_str(), &st) == 0) {
        return 0;
    }

    return -1;
}

std::string RdbHex2Bin(const std::string &hex) {
    std::string out;
    for (auto i: hex) {
        uint8_t n;
        if (i <= '9' and i >= '0')
            n = i - '0';
        else
            n = 10 + i - 'A';
        for (int8_t j = 3; j >= 0; --j)
            out.push_back((n & (1 << j)) ? '1' : '0');
    }

    return out;
}

std::string HexToBinary(const std::string &hexStr) {
    std::string binaryStr = "";

    for (char hexChar: hexStr) {
        // Convert hex character to an integer
        int hexValue = (hexChar >= '0' && hexChar <= '9') ? hexChar - '0' : (hexChar >= 'A' && hexChar <= 'F') ?
                                                                            hexChar - 'A' + 10 : hexChar - 'a' + 10;

        // Convert integer to 4-bit binary and append to the result string
        binaryStr += std::bitset<4>(hexValue).to_string();
    }

    return binaryStr;
}

// Convert a hexadecimal string to raw binary data
void hexToBinaryData(const std::string &hexStr, std::vector<unsigned char> &binaryData) {
    // Ensure the hex string has an even length
    if (hexStr.length() % 2 != 0) {
        perror("Invalid hexadecimal string length.\n");
        return;
    }

    // Loop through the hex string and convert it to binary data
    for (size_t i = 0; i < hexStr.length(); i += 2) {
        // Take two characters at a time, convert them to a byte (unsigned char)
        unsigned char byte = 0;
        if (hexStr[i] >= '0' && hexStr[i] <= '9') {
            byte |= (hexStr[i] - '0') << 4;  // Shift left for the first digit
        } else if (hexStr[i] >= 'A' && hexStr[i] <= 'F') {
            byte |= (hexStr[i] - 'A' + 10) << 4;
        } else if (hexStr[i] >= 'a' && hexStr[i] <= 'f') {
            byte |= (hexStr[i] - 'a' + 10) << 4;
        }

        if (hexStr[i + 1] >= '0' && hexStr[i + 1] <= '9') {
            byte |= (hexStr[i + 1] - '0');  // Second digit
        } else if (hexStr[i + 1] >= 'A' && hexStr[i + 1] <= 'F') {
            byte |= (hexStr[i + 1] - 'A' + 10);
        } else if (hexStr[i + 1] >= 'a' && hexStr[i + 1] <= 'f') {
            byte |= (hexStr[i + 1] - 'a' + 10);
        }

        binaryData.push_back(byte);  // Store the byte in the vector
    }
}