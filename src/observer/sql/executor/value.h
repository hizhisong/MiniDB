/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2021/5/14.
//

#ifndef __OBSERVER_SQL_EXECUTOR_VALUE_H_
#define __OBSERVER_SQL_EXECUTOR_VALUE_H_

#include <string.h>

#include <string>
#include <ostream>

class TupleValue {
public:
  TupleValue() = default;
  virtual ~TupleValue() = default;

  virtual void to_string(std::ostream &os) const = 0;
  virtual std::string to_string() const = 0;
  virtual size_t size() const = 0;
  virtual int compare(const TupleValue &other) const = 0;
private:
};

class IntValue : public TupleValue {
public:
  explicit IntValue(int value) : value_(value) {
  }

  void to_string(std::ostream &os) const override {
    os << value_;
  }

  std::string to_string() const override {
    return std::to_string(value_);
  }

  size_t size() const {
      return sizeof(value_);
  }

  int compare(const TupleValue &other) const override {
    const IntValue & int_other = (const IntValue &)other;
    return value_ - int_other.value_;
  }

private:
  int value_;
};

class FloatValue : public TupleValue {
public:
  explicit FloatValue(float value) : value_(value) {
  }

  void to_string(std::ostream &os) const override {
    os << value_;
  }
  std::string to_string() const override {
      return std::to_string(value_);
  }

  size_t size() const {
      return sizeof(value_);
  }

  int compare(const TupleValue &other) const override {
    const FloatValue & float_other = (const FloatValue &)other;
    float result = value_ - float_other.value_;
    if (result > 0) { // 浮点数没有考虑精度问题
      return 1;
    }
    if (result < 0) {
      return -1;
    }
    return 0;
  }
private:
  float value_;
};

class StringValue : public TupleValue {
public:
  StringValue(const char *value, int len) : value_(value, len){
  }
  explicit StringValue(const char *value) : value_(value) {
  }

  void to_string(std::ostream &os) const override {
    os << value_;
  }

  std::string to_string() const override {
      return value_;
  }

  size_t size() const {
      return value_.size();
  }

  int compare(const TupleValue &other) const override {
    const StringValue &string_other = (const StringValue &)other;
    return strcmp(value_.c_str(), string_other.value_.c_str());
  }
private:
  std::string value_;
};

class DateValue : public TupleValue {
public:
    explicit DateValue(int date_int_format): date_int_format_(date_int_format) {}

    void to_string(std::ostream &os) const override {
        char date_str[11] = "0000-00-00";
        int date_int = date_int_format_;
        for (int i = 9; i >= 0; i--) {
            if (date_str[i] != '-') {
                int tmp = date_int % 10;
                date_int /= 10;
                date_str[i] = tmp + '0';
            }
        }
        os << date_str;
    }

    std::string to_string() const override {
        return std::to_string(date_int_format_);
    }

    size_t size() const {
        return sizeof(date_int_format_);
    }

    int compare(const TupleValue &other) const override {
        const DateValue & date_other = (const DateValue &)other;
        return date_int_format_ - date_other.date_int_format_;
    }

private:
    int date_int_format_;
};


#endif //__OBSERVER_SQL_EXECUTOR_VALUE_H_
