/***********************************************

   Distributed Systems: Paradigms and models
   2015/2016 Final project source code
   Micro MDF
   Author: Andrea Maggiordomo

************************************************/

#ifndef MDF_MDF_HPP
#define MDF_MDF_HPP

#include <unordered_map>
#include <vector>
#include <string>
#include <memory>
#include <mutex>
#include <thread>
#include <atomic>

#include "Graph.hpp"
#include "Token.hpp"
#include "ConcurrentQueue.hpp"
#include "ConcurrentMap.hpp"
#include "Printer.hpp"

namespace mdf {

struct InputTokenContainer {

	ParameterAddress destination;
	TokenHandle token;

	InputTokenContainer(NodeId destId, std::string pname, TokenHandle tk)
			: destination{destId, pname}, token{tk} { }

};

template<typename D>
class Mdf {

private:

	class InstructionState {
	
	private:

		std::mutex _mtx;

	public:

		bool fired;
		unsigned resolvedDependencies;
		std::unordered_map<std::string,TokenHandle> tokens;

		void lock() { _mtx.lock(); }
		void unlock() { _mtx.unlock(); }

	};

	struct GraphHandle {
		const std::size_t instanceId;
		std::shared_ptr<Graph> graph;
		mdf::ConcurrentMap<NodeId,std::shared_ptr<InstructionState>> states;

		GraphHandle(std::size_t iid, std::shared_ptr<Graph> g) : instanceId{iid}, graph{g}, states{} { } 
	};

	struct TaskData {
		std::shared_ptr<GraphHandle> gh;
		NodeId id;
	};

	using TaskQueue = mdf::ConcurrentQueue<TaskData>;

	std::unique_ptr<Graph> _model;

	std::size_t _tn; // Number of active threads
	TaskQueue _tasks;
	std::vector<std::thread> _threads;
	std::vector<std::unique_ptr<TaskQueue>> _localTasks;

	std::atomic<long> _numInstances; // Number of active graph instances
	std::atomic<bool> _endOfStream;

	/*
	 * During the execution we acquire unique ownership
	 * of the drainer, in case it needs to access critical resources 
	 * like memory, files etc...
	 */
	std::unique_ptr<D> _drainer;
	std::mutex _drainerMutex;

public:

	Mdf(std::unique_ptr<Graph> model, std::size_t tn, std::unique_ptr<D> drainer);
	Mdf(const Graph& model, std::size_t tn, std::unique_ptr<D> drainer);
	Mdf(const Mdf& other) = delete;
	Mdf& operator=(const Mdf& other) = delete;

	template<typename S>
		std::unique_ptr<S> Start(std::unique_ptr<S> streamer);

private:

	void Worker(std::size_t index);
	bool Steal(TaskData& t, std::size_t shuffle);
	void ScheduleIfFireable(std::shared_ptr<GraphHandle> gh, NodeId id, TaskQueue& queue);
	
};

template<typename D>
inline Mdf<D>::Mdf(std::unique_ptr<Graph> model, std::size_t tn, std::unique_ptr<D> drainer)
		: _model{std::move(model)},
		  _tn{tn},
		  _tasks{100},
		  _threads{},
		  _localTasks{},
		  _numInstances{0},
		  _endOfStream{true},
		  _drainer{std::move(drainer)},
		  _drainerMutex{}
{
	_threads.reserve(_tn);
	_localTasks.reserve(_tn);

	for (std::size_t i = 0; i < _tn; ++i) {
		_localTasks.emplace_back(std::unique_ptr<TaskQueue>(new TaskQueue{}));
	}
}

template<typename D>
inline Mdf<D>::Mdf(const Graph& model, std::size_t tn, std::unique_ptr<D> drainer)
		: Mdf{std::move(std::unique_ptr<Graph>{new Graph{model}}), tn, std::move(drainer)}
{
}

template<typename D> template<typename S>
inline std::unique_ptr<S> Mdf<D>::Start(std::unique_ptr<S> streamer)
{
	_endOfStream = false;

	out.Println("Starting threads...");

	for (std::size_t i = 0; i < _tn; ++i) {
		_threads.emplace_back(std::thread{&Mdf::Worker, this, i});
	}

	std::size_t count = 0;

	while (!_endOfStream) {

		std::vector<InputTokenContainer> inputTokens = streamer->Next();
		if (inputTokens.size() > 0) {
			std::shared_ptr<GraphHandle> gh = std::make_shared<GraphHandle>(count++, _model->Clone());
			++_numInstances;
			for (auto& itc : inputTokens) {
				auto pair = gh->states.Get(itc.destination.nodeId);
				auto state = pair.second ? pair.first : gh->states.Insert(itc.destination.nodeId, std::make_shared<InstructionState>()).first;
				std::lock_guard<InstructionState> lock{*state};
				state->tokens[itc.destination.paramName] = itc.token;
				ScheduleIfFireable(gh, itc.destination.nodeId, _tasks);
			}
		} else {
			_endOfStream = true;
		}
	}

	out.Println("Joining threads...");

	for (std::size_t i = 0; i < _tn; ++i) {
		if (_threads[i].joinable()) _threads[i].join();
	}

	out.Println("Finished.");

	return streamer;
}

template <typename D>
inline void Mdf<D>::ScheduleIfFireable(std::shared_ptr<GraphHandle> gh, NodeId id, TaskQueue& queue)
{
	auto state = gh->states.Get(id).first;
	auto node = gh->graph->GetNode(id);
	if (state->resolvedDependencies == node->numDependsOn && state->tokens.size() == node->instruction->Arity()
			&& state->fired == false) {
		state->fired = true;
		queue.Put(TaskData{gh, id});
	}
}


template<typename D>
inline bool Mdf<D>::Steal(TaskData& t, std::size_t shuffle)
{
	for (std::size_t i = 0; i < _tn; ++i) {
		std::size_t idx = (shuffle+i+1)%_tn;
		if (_localTasks[idx]->Get(t)) return true;
	}
	return false;
}

template<typename D>
inline void Mdf<D>::Worker(std::size_t index)
{
	out.Println("Worker running with index ", index);
	TaskQueue& localTasks = *_localTasks[index];
	TaskData t;
	while (true) {
		if (localTasks.Get(t) || _tasks.Get(t) || Steal(t, index)) {
			auto node = t.gh->graph->GetNode(t.id);
			auto state = t.gh->states.Get(t.id).first;
			assert(state);

			auto res = node->instruction->Execute(state->tokens);

			if (node->links.size() == 0 && node->dependentNodes.size() == 0) {
				{
					std::lock_guard<std::mutex> lock{_drainerMutex};
					(*_drainer)(res);
				}
				int n = --_numInstances;
				assert(n >= 0);
			} else {
				// Count dependencies and fire instructions that do not require the result
				for (auto& dependentId : node->dependentNodes) {
					auto pair = t.gh->states.Get(dependentId);
					auto state = pair.second ? pair.first : t.gh->states.Insert(dependentId, std::make_shared<InstructionState>()).first;
					std::lock_guard<InstructionState> lock{*state};
					state->resolvedDependencies++;
					ScheduleIfFireable(t.gh, dependentId, localTasks);
				}

				// Move the result and create tasks for any new fireable instruction
				for (auto& target : node->links) {
					auto pair = t.gh->states.Get(target.nodeId);
					auto state = pair.second ? pair.first : t.gh->states.Insert(target.nodeId, std::make_shared<InstructionState>()).first;
					std::lock_guard<InstructionState> lock{*state};
					state->tokens[target.paramName] = res;
					ScheduleIfFireable(t.gh, target.nodeId, localTasks);
				}
			}
		} else {
			if (!_endOfStream || _numInstances > 0) {
				std::this_thread::yield();
			}
			else return;
		}
	}
}

} // mdf namespace

#endif

