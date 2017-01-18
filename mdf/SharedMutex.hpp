/***********************************************

   Distributed Systems: Paradigms and models
   2015/2016 Final project source code
   Micro MDF
   Author: Andrea Maggiordomo

************************************************/

#ifndef MDF_SHARED_MUTEX_HPP
#define MDF_SHARED_MUTEX_HPP

#include <mutex>

namespace ext {

class shared_mutex {

private:

	std::mutex _exclusive_mutex;
	std::mutex _counter_mutex;
	int _shared_counter;

public:

	shared_mutex() : _exclusive_mutex{}, _counter_mutex{}, _shared_counter{0} { }
	shared_mutex(const shared_mutex&) = delete;
	shared_mutex operator=(const shared_mutex &) = delete;

	void lock()
	{
		_exclusive_mutex.lock();
	}

	void unlock()
	{
		_exclusive_mutex.unlock();
	}

	void lock_shared()
	{
		std::lock_guard<std::mutex> lock{_counter_mutex};
		if (++_shared_counter == 1)
			_exclusive_mutex.lock();
	}

	void unlock_shared()
	{
		std::lock_guard<std::mutex> lock{_counter_mutex};
		if (--_shared_counter == 0)
			_exclusive_mutex.unlock();
	}
};

template <typename S>
class shared_lock_guard {

private:

	S& _mutex;

public:

	shared_lock_guard(S& mutex) : _mutex{mutex}
	{
		_mutex.lock_shared();
	}

	~shared_lock_guard()
	{
		_mutex.unlock_shared();
	}

};

} // namespace ext

#endif 

