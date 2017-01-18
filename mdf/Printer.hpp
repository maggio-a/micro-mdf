/***********************************************

   Distributed Systems: Paradigms and models
   2015/2016 Final project source code
   Micro MDF
   Author: Andrea Maggiordomo

************************************************/

#ifndef MDF_PRINTER_HPP
#define MDF_PRINTER_HPP 

#include <sstream>
#include <iostream>
#include <mutex>

namespace mdf {

class Printer {

private:

	std::ostream& _os;
	std::mutex _mtx;

public:

	Printer(std::ostream& os) : _os{os}, _mtx{} { }
			
	template <typename... T> void Println(T&&... args)
	{
		std::lock_guard<std::mutex> lck{_mtx};
		PrintRec(std::forward<T>(args)...);
		_os << std::endl;
	}

	template <typename... T> void Print(T&&... args)
	{
		std::lock_guard<std::mutex> lck{_mtx};
		PrintRec(std::forward<T>(args)...);
	}

private:

	template<typename T, typename... R> void PrintRec(T&& arg, R&&... rest)
	{
		_os << arg;
		PrintRec(std::forward<R>(rest)...);
	}

	void PrintRec() { }
};

Printer out{std::cout};
Printer err{std::cerr};

} // mdf namespace

#endif

