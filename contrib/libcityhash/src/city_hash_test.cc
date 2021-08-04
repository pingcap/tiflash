#include <iostream>

#include "city.h"

int main(int argc, char * argv[])
{
    const size_t buf_sz = 66;
    char c_buff[buf_sz];

    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i & 0xff;
    }
    std::cout << CityHash_v1_0_2::CityHash64(c_buff, buf_sz) << std::endl;
}