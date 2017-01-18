/***********************************************

   Distributed Systems: Paradigms and models
   2015/2016 Final project source code
   Micro MDF
   Author: Andrea Maggiordomo

************************************************/

#ifndef MDF_CONCURRENT_MAP_HPP
#define MDF_CONCURRENT_MAP_HPP

#include <unordered_map>
#include <mutex>
#include <utility>
#include <algorithm>

#include "SharedMutex.hpp"

namespace mdf {

template <typename K, typename V, typename H = std::hash<K>, typename P = std::equal_to<K>>
class ConcurrentMap {

public:

	using Key = K;
	using Value = V;
	using Hash = H;
	using Predicate = P;
	using ReturnType = std::pair<Value,bool>;

private:

	struct Bucket {

		using BucketElement = std::pair<Key,Value>;
		using BucketData = std::vector<BucketElement>;
		using BucketIterator = typename BucketData::iterator;
		using ConstBucketIterator = typename BucketData::const_iterator;
	
		std::vector<BucketElement> data;
		Predicate _eq;
		mutable ext::shared_mutex bucketMutex;

		BucketIterator Find(const Key& key)
		{
			return std::find_if(data.begin(), data.end(), [&] (const BucketElement& e) { return _eq(e.first, key); });
		}

		ConstBucketIterator Find(const Key& key) const
		{
			return std::find_if(data.begin(), data.end(), [&] (const BucketElement& e) { return _eq(e.first, key); });
		}
		
		ReturnType Get(const Key& key) const
		{
			ext::shared_lock_guard<ext::shared_mutex> lock{bucketMutex};
			auto it = Find(key);
			if (it != data.end())
				return std::make_pair(it->second, true);
			else
				return std::make_pair(Value{}, false);
		}

		void Remove(const Key& key)
		{
			std::lock_guard<ext::shared_mutex> lock{bucketMutex};
			auto it = Find(key);
			if (it != data.end()) data.erase(it);
		}

		/* This guarantees only basic exception safety (the map will
		 * remain in a consistent state) but it doesn't roll back a
		 * possible insertion if the 'else' branch throws after the
		 * BucketElement insertion
		 */
		ReturnType Insert(const Key& key, const Value& val)
		{
			std::lock_guard<ext::shared_mutex> lock{bucketMutex};
			auto it = Find(key);
			if (it != data.end()) {
				return std::make_pair(it->second, false);
			} else {
				data.emplace_back(BucketElement{key, val});
				return std::make_pair(data.back().second, true);
			} 
		}

	};

	std::vector<std::unique_ptr<Bucket>> _buckets;
	Hash _hash;

public:

	ConcurrentMap(unsigned size=11, const Hash& hash=H{}) : _buckets{}, _hash{hash}
	{
		assert(size);
		_buckets.reserve(size);
		for (unsigned int i = 0; i < size; ++i)
			_buckets.push_back(std::unique_ptr<Bucket>{new Bucket});
	}

	ConcurrentMap(const ConcurrentMap<K,V,H,P>&) = delete;

	ConcurrentMap<K,V,H,P>& operator=(const ConcurrentMap<K,V,H,P>&) = delete;

	ReturnType Get(const Key& key) const
	{
		return GetBucket(key).Get(key);
	}

	void Remove(const Key& key)
	{
		return GetBucket(key).Remove(key);
	}

	ReturnType Insert(const Key& key, const Value& val)
	{
		return GetBucket(key).Insert(key, val);
	}

	std::size_t Size() const
	{
		std::size_t size;
		std::vector<std::unique_lock<ext::shared_mutex>> locks;
		for (std::size_t i = 0; i < _buckets.size(); ++i) {
			locks.emplace_back(std::unique_lock<ext::shared_mutex>{_buckets[i]->bucketMutex});
			size += _buckets[i]->data.size();
		}
		return size;
	}
	
private:

	Bucket& GetBucket(const Key& key) const
	{
		return *_buckets[_hash(key)%_buckets.size()];
	}

};

} // mdf namespace

#endif

