//
// Created by arenatlx on 2022/10/27.
//

#include <iostream>
#include <vector>
#include "tai.h"



class HAHA {
public:
    template <class T>
    HAHA & operator=(std::vector<T> && rhs){
        return rhs[0];
    }
private:
    int a;
};


class MY {
    using Arra1y = std::vector<HAHA>;
public:
    HAHA operator[](size_t n) const;
};

HAHA MY::operator[](size_t n) const{
    Arra1y a(n);
    return a[0];
}

struct Test{
    ~Test(){
        std::cout<<"kill test"<<std::endl;
    }
};


int main(int argc, char* argv[]){
    std::cout<<__FILE_NAME__<< __DATE__ << std::endl;
    MY* m = new(MY);
    m[1];
    {
        auto vec = std::vector<Test>();
        auto tmp = new(Test);    // 这个地方直接 Test() 还是会析构一次，有点奇怪，只有 new 才行。
        vec.push_back(std::move(*tmp));
        std::cout<<vec.size()<<std::endl;
    }
    std::string a = "123";
    auto cp_a = a;
    cp_a[0] = '4';
    std::cout<<cp_a<<std::endl;
    std::cout<<a<<std::endl;

    std::vector<std::string> v;
    v.push_back("aaaa");
    v.push_back("bbbb");
    v[0][0]='1';
    v.push_back(std::move(v[0]));
    std::cout<<v[0]<<std::endl;
    std::cout<<v[1]<<std::endl;
    std::cout<<v[2]<<std::endl;
}