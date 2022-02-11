#pragma once

#include <exception>
#include <string>

class Exception : std::exception {
 public:
  const char * what() const noexcept override { return msg_.data(); }

  explicit Exception(std::string msg) : msg_(std::move(msg)) {}

 private:
  std::string msg_;
};