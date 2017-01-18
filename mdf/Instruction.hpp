/***********************************************

   Distributed Systems: Paradigms and models
   2015/2016 Final project source code
   Micro MDF
   Author: Andrea Maggiordomo

************************************************/

#ifndef MDF_INSTRUCTION_HPP
#define MDF_INSTRUCTION_HPP

#include "Token.hpp"

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>

namespace mdf {

template<typename T>
struct ParamDecl
{
	using Type = T;
	const std::string name;
	ParamDecl(std::string n) : name(n) { }
};

class Instruction {
public:
	Instruction() { }

	virtual ~Instruction() { }

	virtual std::shared_ptr<Token> Execute(const std::unordered_map<std::string,TokenHandle>& inputTokens) const = 0;
	virtual std::size_t Arity() const = 0;

	virtual std::shared_ptr<Instruction> Clone() const = 0;
};



namespace detail {

/*
 * Recursive template to invoke the instruction code
 * f is the function to call, m is the <Name,Token> map, T is the tuple of parameter
 * declarations (each declaration is a ParamDecl<U> struct containing the name of the
 * token linked to the parameter and the type ParamDecl<U>::Type of the parameter)
 * p... is the list of unpacked parameters
 * Adapted from: http://stackoverflow.com/a/12650100/125717
 */
template<size_t N> struct Unpack
{
	template<typename F, typename T, typename... P>
	static auto unpack(F f, const std::unordered_map<std::string,TokenHandle> &m, T t, P... p)
		-> decltype(Unpack<N-1>::unpack(f, m, t, std::static_pointer_cast<Value<typename std::tuple_element<N-1,T>::type::Type>>(m.at(std::get<N-1>(t).name))->GetValue(), p...))
	{
		return Unpack<N-1>::unpack(f, m, t, std::static_pointer_cast<Value<typename std::tuple_element<N-1,T>::type::Type>>(m.at(std::get<N-1>(t).name))->GetValue(), p...);
	}
};

template<> struct Unpack<0>
{
	template<typename F, typename T, typename... P>
	static auto unpack(F f, const std::unordered_map<std::string,TokenHandle>& m, T t, P... p)
		-> decltype(f(p...))
	{
		return f(p...);
	}
};

template<typename F, typename... ArgTypes>
auto Call(F f, const std::unordered_map<std::string,TokenHandle>& m, std::tuple<ArgTypes...> args)
	-> decltype(Unpack<std::tuple_size<decltype(args)>::value>::unpack(f, m, args))
{
	return Unpack<std::tuple_size<decltype(args)>::value>::unpack(f, m, args);
}

template<typename F, typename... ArgTypes>
class InstructionImpl : public Instruction {

	F _fct;
	const std::tuple<ParamDecl<ArgTypes>...> _args;
	const std::size_t _n;

public:

	InstructionImpl(F f, ParamDecl<ArgTypes>... args) : _fct{f}, _args{std::make_tuple(args...)},
		_n{std::tuple_size<std::tuple<ParamDecl<ArgTypes>...>>::value} { }
	
	InstructionImpl(const InstructionImpl<F,ArgTypes...>& other) : _fct{other._fct}, _args{other._args}, _n(other._n) { }

	std::shared_ptr<Token> Execute(const std::unordered_map<std::string,TokenHandle>& inputTokens) const
	{
		auto r = Call(_fct, inputTokens, _args);
		return std::make_shared<Value<decltype(r)>>(r);
	}

	std::size_t Arity() const
	{
		return _n;
	}

	std::shared_ptr<Instruction> Clone() const
	{
		return std::make_shared<InstructionImpl<F,ArgTypes...>>(*this);
	}

};

} // detail namespace 



template<typename F, typename... T>
std::shared_ptr<Instruction> MakeInstruction(F f, ParamDecl<T>... params)
{
	return std::make_shared<detail::InstructionImpl<F, T...>>(f, params...);
}

} // mdf namespace

#endif 

