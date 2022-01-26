//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// exception.h
//
// Identification: src/include/common/exception.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include "type/type.h"

namespace bustub {

// TODO(WAN): the comment I added below is a lie, but you shouldn't need to poke around here. Don't worry about it.
//  Most of the exception types are type subsystem madness. I think we can get rid of it at some point.
/** ExceptionType is all the types of exceptions that we expect to throw in our system. */
enum class ExceptionType {
  /** Invalid exception type.*/
  INVALID = 0,
  /** Value out of range. */
  OUT_OF_RANGE = 1,
  /** Conversion/casting error. */
  CONVERSION = 2,
  /** Unknown type in the type subsystem. */
  UNKNOWN_TYPE = 3,
  /** Decimal-related errors. */
  DECIMAL = 4,
  /** Type mismatch. */
  MISMATCH_TYPE = 5,
  /** Division by 0. */
  DIVIDE_BY_ZERO = 6,
  /** Incompatible type. */
  INCOMPATIBLE_TYPE = 8,
  /** Out of memory error */
  OUT_OF_MEMORY = 9,
  /** Method not implemented. */
  NOT_IMPLEMENTED = 11,
};

class Exception : public std::runtime_error {
 public:
  /**
   * Construct a new Exception instance.
   * @param message The exception message
   */
  explicit Exception(const std::string &message) : std::runtime_error(message), type_(ExceptionType::INVALID) {
    std::string exception_message = "Message :: " + message + "\n";
    std::cerr << exception_message;
  }

  /**
   * Construct a new Exception instance with specified type.
   * @param exception_type The exception type
   * @param message The exception message
   */
  Exception(ExceptionType exception_type, const std::string &message)
      : std::runtime_error(message), type_(exception_type) {
    std::string exception_message =
        "\nException Type :: " + ExceptionTypeToString(type_) + "\nMessage :: " + message + "\n";
    std::cerr << exception_message;
  }

  /** @return The type of the exception */
  ExceptionType GetType() const { return type_; }

  /** @return A human-readable string for the specified exception type */
  static std::string ExceptionTypeToString(ExceptionType type) {
    switch (type) {
      case ExceptionType::INVALID:
        return "Invalid";
      case ExceptionType::OUT_OF_RANGE:
        return "Out of Range";
      case ExceptionType::CONVERSION:
        return "Conversion";
      case ExceptionType::UNKNOWN_TYPE:
        return "Unknown Type";
      case ExceptionType::DECIMAL:
        return "Decimal";
      case ExceptionType::MISMATCH_TYPE:
        return "Mismatch Type";
      case ExceptionType::DIVIDE_BY_ZERO:
        return "Divide by Zero";
      case ExceptionType::INCOMPATIBLE_TYPE:
        return "Incompatible type";
      case ExceptionType::OUT_OF_MEMORY:
        return "Out of Memory";
      case ExceptionType::NOT_IMPLEMENTED:
        return "Not implemented";
      default:
        return "Unknown";
    }
  }

 private:
  ExceptionType type_;
};

class NotImplementedException : public Exception {
 public:
  NotImplementedException() = delete;
  explicit NotImplementedException(const std::string &msg) : Exception(ExceptionType::NOT_IMPLEMENTED, msg) {}
};

class OutOfRangeException : public Exception {
 public:
  OutOfRangeException() = delete;
  explicit OutOfRangeException(const std::string &msg) : Exception(ExceptionType::OUT_OF_RANGE, msg) {}
};

}  // namespace bustub
