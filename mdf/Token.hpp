/***********************************************

   Distributed Systems: Paradigms and models
   2015/2016 Final project source code
   Micro MDF
   Author: Andrea Maggiordomo

************************************************/

#ifndef MDF_TOKEN_HPP
#define MDF_TOKEN_HPP 

#include <memory>

namespace mdf {

class Token { // Erasure class

public:
	
	virtual ~Token() { }

};

template <typename T> class Value : public Token {

private:

	T _val;

public:

	Value(T v) : _val(v) { }
	Value(const Value<T>& other) : _val(other._val) { }

	~Value() { }

	T GetValue() { return _val; }

};

using TokenHandle = std::shared_ptr<Token>;

template <typename V>
	using ValueHandle = std::shared_ptr<Value<V>>;

template<typename T> ValueHandle<T> WrapValue(T val)
{
	auto x = std::make_shared<Value<T>>(val);
	return x;
}

} // mdf namespace

#endif

