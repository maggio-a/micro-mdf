/***********************************************

   Distributed Systems: Paradigms and models
   2015/2016 Final project source code
   Micro MDF
   Author: Andrea Maggiordomo

************************************************/

#ifndef MDF_GRAPH_HPP
#define MDF_GRAPH_HPP

#include "Mdf.hpp"
#include "Instruction.hpp"

#include <utility>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <vector>

#include <stdexcept>
#include <cassert>

namespace mdf {

using NodeId = std::size_t;

struct ParameterAddress {

	NodeId nodeId;
	std::string paramName;

	ParameterAddress(NodeId id, std::string pname) : nodeId{id}, paramName{pname} { }

	bool operator==(const ParameterAddress& other) const
	{
		return nodeId == other.nodeId && paramName == other.paramName;
	}

};

class Node {

	friend class Graph;
	template<typename D> friend class Mdf;

	struct AddressHash {
		inline size_t operator()(const ParameterAddress& pa) const
		{
			return std::hash<NodeId>{}(pa.nodeId)^std::hash<std::string>{}(pa.paramName);
		}
	};

public:
	
	const NodeId id;

private:

	std::shared_ptr<Instruction> instruction;
	std::unordered_set<ParameterAddress,AddressHash> links;
	std::unordered_set<NodeId> dependentNodes; // Nodes that depend on 'this'
	unsigned numDependsOn; // Number of nodes that 'this' depends on

public:

	Node(NodeId iid, std::shared_ptr<Instruction> instr) : id(iid), instruction{instr}, links{}, dependentNodes{}, numDependsOn{0} { }

	Node(const Node& other) = delete;
	Node& operator=(const Node& other) = delete;

private:

	Node(NodeId i, std::shared_ptr<Instruction> ins, const std::unordered_set<ParameterAddress,AddressHash>& l,
			const std::unordered_set<NodeId>& d, unsigned ndo) : id(i), instruction{ins}, links{l}, dependentNodes{d}, numDependsOn{ndo} { }

public:

	std::shared_ptr<Node> Clone() const
	{
		return std::shared_ptr<Node>(new Node{id, instruction->Clone(), links, dependentNodes, numDependsOn});
	}

};


class Graph {

	std::vector<std::shared_ptr<Node>> _instructions;

public:

	Graph() : _instructions{} { }
	Graph(const Graph& other) : _instructions{}
	{
		_instructions.reserve(other._instructions.size());
		for (std::size_t i = 0; i < other._instructions.size(); ++i)
			_instructions.push_back(other._instructions[i]->Clone());
	}

	template<typename F, typename... T>
	NodeId AddInstruction(F f, ParamDecl<T>... params)
	{
		auto instruction = MakeInstruction(f, params...);
		NodeId id = _instructions.size();
		_instructions.push_back(std::make_shared<Node>(id, instruction));
		return id;
	}

	bool Connect(NodeId src, NodeId dest, std::string pname)
	{
		assert(_instructions.size() > src && _instructions.size() > dest);
		return (_instructions[src]->links).insert(ParameterAddress{dest, pname}).second;
	}
	
	void DeclareDependency(NodeId src, NodeId dest)
	{
		assert(_instructions.size() > src && _instructions.size() > dest);
		auto it = (_instructions[src]->dependentNodes).insert(dest);
		if (it.second == true) _instructions[dest]->numDependsOn++;
	}

	std::shared_ptr<Node> GetNode(NodeId id)
	{
		return _instructions.at(id);
	}

	std::shared_ptr<Node> operator[](NodeId id)
	{
		return _instructions.at(id);
	}

	std::shared_ptr<Graph> Clone() const
	{
		return std::make_shared<Graph>(*this);
	}

	std::size_t N() const
	{
		return _instructions.size();
	}

};

}

#endif

