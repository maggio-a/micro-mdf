/***********************************************

   Distributed Systems: Paradigms and models
   2015/2016 Final project source code
   Micro MDF
   Author: Andrea Maggiordomo

************************************************/

#ifndef MDF_CONCURRENT_QUEUE_HPP
#define MDF_CONCURRENT_QUEUE_HPP

#include <mutex>
#include <deque>
#include <condition_variable>

namespace mdf {

template <typename T>
class ConcurrentQueue {

private:

	const std::size_t _capacity; // 0 means unbounded
	std::mutex _mtx;
	std::condition_variable _resume;
	std::deque<T> _deque;

public:	

	ConcurrentQueue(std::size_t capacity=0) : _mtx{}, _deque{}, _capacity{capacity} { }
	ConcurrentQueue(const ConcurrentQueue<T>& other) = delete;	
	ConcurrentQueue<T>& operator=(const ConcurrentQueue<T>& other) = delete;

	bool IsEmpty()
	{
		std::lock_guard<std::mutex> lock{_mtx};
		return _deque.empty();
	}

	void Put(const T& v)
	{
		std::unique_lock<std::mutex> lock{_mtx};
		while (_capacity > 0 && _deque.size() > _capacity)
			_resume.wait(lock);
		_deque.push_back(v);
	}

	bool Get(T& v)
	{
		
		std::lock_guard<std::mutex> lock{_mtx};
		if (!_deque.empty()) {
			v = _deque.front();
			_deque.pop_front();
			if (_capacity > 0 && _deque.size() < 0.25*_capacity)
				_resume.notify_all();
			return true;
		} else return false;
	}

};

} // mdf namespace

#endif

