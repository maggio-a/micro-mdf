/***********************************************

   Distributed Systems: Paradigms and models
   2015/2016 Final project source code
   Micro MDF
   Author: Andrea Maggiordomo

************************************************/

#include <iostream>
#include <sstream>
#include <cstdlib>
#include <cmath>

#include "../mdf/Mdf.hpp"
#include "../mdf/Printer.hpp"

using namespace std;

using mdf::InputTokenContainer;

int Foo(int x, double y)
{
	return x%2 ? int(floor(y)) : int(ceil(y));
}

int Bar(int z)
{
	return z*2;
}

class Streamer {

	mdf::NodeId idFoo;

public:

	Streamer(mdf::NodeId id, int seed) : idFoo{id} { srand(seed); }

	vector<InputTokenContainer> Next()
	{
		vector<InputTokenContainer> input;
		if (!endOfInput()) {
			input.emplace_back(InputTokenContainer{idFoo, "p1Foo", mdf::WrapValue<int>(NextInt())});
			input.emplace_back(InputTokenContainer{idFoo, "p2Foo", mdf::WrapValue<double>(NextDouble())});
		}
		return input;
	}

private:

	bool endOfInput() { return NextDouble() > 9.9; }
	int NextInt() { return rand()%100; }
	double NextDouble() { return (rand()/(double)RAND_MAX) * 10.0; }
};

class Drainer {

public:

	void operator()(mdf::TokenHandle token)
	{
		mdf::ValueHandle<int> out = dynamic_pointer_cast<mdf::Value<int>>(token);
		if (out) {
			int r = out->GetValue();
			mdf::out.Println("Drainer value: ", r);
		} else {
			abort();
		}
	}
};

int main(int argc, char *argv[])
{
	unsigned long tn = (argc>1) ? stoul(argv[1]) : 2;
	int seed = (argc>2) ? stoi(argv[2]) : 0;

	mdf::Graph g{};

	mdf::NodeId idFoo = g.AddInstruction(&Foo, mdf::ParamDecl<int>{"p1Foo"}, mdf::ParamDecl<double>{"p2Foo"});
	mdf::NodeId idBar = g.AddInstruction(&Bar, mdf::ParamDecl<int>{"p1Bar"});

	g.Connect(idFoo, idBar, "p1Bar");

	mdf::Mdf<Drainer> interpreter{g, tn, unique_ptr<Drainer>{new Drainer}};

	unique_ptr<Streamer> streamer{new Streamer{idFoo, seed}};

	streamer = interpreter.Start(move(streamer));

	return 0;
	
}

