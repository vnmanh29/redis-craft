//
// Created by Manh Nguyen Viet on 7/21/25.
//

#include "Utils.h"
#include "all.hpp"

std::string EncodeArr2RespArr(std::vector<std::string>& arr)
{
    std::string resp_str;
    resp::encoder<std::string> enc;
    auto encoded_arr = enc.encode_arr(arr);
    for (auto& encoded : encoded_arr)
    {
        resp_str += encoded;
    }
    return resp_str;
}