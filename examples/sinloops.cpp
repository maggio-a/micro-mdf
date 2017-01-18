/***********************************************

   Distributed Systems: Paradigms and models
   2015/2016 Final project source code
   Micro MDF
   Author: Andrea Maggiordomo

************************************************/

/*
 * Mdf example
 *
 * Graph topology:
 *
 *         i_1
 *     +----+----+
 *     |    |    |
 *    i_2  i_3  i_4
 *     \   /    /
 *      i_5    /
 *        \   /
 *         i_6
 */

#include <iostream>
#include <cmath>
#include <utility>

#include "../mdf/Mdf.hpp"

const double PI = 3.14159265358979323846;

using namespace std;

class Drainer {

public:

	void operator()(mdf::TokenHandle token)
	{
		mdf::ValueHandle<pair<int,double>> result = dynamic_pointer_cast<mdf::Value<pair<int,double>>>(token);
		if (result)	{
			auto val = result->GetValue();
		} else
			mdf::out.Println("Drainer: downcast failed.");
	}

};

class Streamer {

private:

	mdf::NodeId _i1;
	mdf::NodeId _i6;
	const int _maxItems;
	int _numItems;

public:

	Streamer(mdf::NodeId i1, mdf::NodeId i6, int maxItems) : _i1{i1}, _i6{i6}, _maxItems{maxItems}, _numItems{0}
	{
		assert(_maxItems > 0);
	}

	vector<mdf::InputTokenContainer> Next()
	{
		vector<mdf::InputTokenContainer> input;
		if (_numItems++ < _maxItems) {
			input.emplace_back(mdf::InputTokenContainer{_i1, "input1", mdf::WrapValue<int>(_numItems)});
			input.emplace_back(mdf::InputTokenContainer{_i1, "input2", mdf::WrapValue<double>(PI/(double)_numItems)});
			input.emplace_back(mdf::InputTokenContainer{_i6, "counter", mdf::WrapValue<int>(_numItems)});
		}
		return input;
	}

};

int main(int argc, char *argv[])
{
	try {
	
	int numItems = (argc>1) ? stoi(argv[1]) : 100;
	unsigned long tn = (argc>2) ? stoul(argv[2]) : 1;
	unsigned long n = (argc>3) ? stoul(argv[3]) : 100;

	mdf::out.Println("Streaming ", numItems, " items, running ", tn, " threads, looping ", n, " times in each funcion.");

	mdf::Graph g{};

	mdf::NodeId i1 = g.AddInstruction(
			[n](int in1, double in2) -> double {
				double x = in1+in2;
				for (unsigned i = 0; i < n; ++i)
					x = std::sin(x);
				return x;
			},
		   	mdf::ParamDecl<int>{"input1"},
			mdf::ParamDecl<double>{"input2"});

	mdf::NodeId i2 = g.AddInstruction(
			[n](double x) -> double {
				double y1 = x + 1.0;
				for (unsigned i = 0; i < n; ++i)
					y1 = std::sin(y1);
				return y1;
			},
			mdf::ParamDecl<double>{"x"});

	mdf::NodeId i3 = g.AddInstruction(
			[n](double x) -> double {
				double y2 = x + 2.0;
				for (unsigned i = 0; i < n; ++i)
					y2 = std::sin(y2);
				return y2;
			},
			mdf::ParamDecl<double>{"x"});

	mdf::NodeId i4 = g.AddInstruction(
			[n](double x) -> double {
				double z = x + 3.0;
				for (unsigned i = 0; i < n; ++i)
					z = std::sin(z);
				return z;
			},
			mdf::ParamDecl<double>{"x"});
	
	mdf::NodeId i5 = g.AddInstruction(
			[n](double y1, double y2) -> double {
				double y = y1 + y2 + 4.0;
				for (unsigned i = 0; i < n; ++i)
					y = std::sin(y);
				return y;
			},
			mdf::ParamDecl<double>{"y1"},
			mdf::ParamDecl<double>{"y2"});

	mdf::NodeId i6 = g.AddInstruction(
			[n](double y, double z, int c) -> pair<int,double> {
				double w = y + z + 5.0;
				for (unsigned i = 0; i < n; ++i)
					w = std::sin(w);
				return make_pair(c,w);
			},
			mdf::ParamDecl<double>{"y"},
			mdf::ParamDecl<double>{"z"},
			mdf::ParamDecl<int>{"counter"});

	g.Connect(i1, i2, "x");
	g.Connect(i1, i3, "x");
	g.Connect(i1, i4, "x");
	g.Connect(i2, i5, "y1");
	g.Connect(i3, i5, "y2");
	g.Connect(i5, i6, "y");
	g.Connect(i4, i6, "z");

	unique_ptr<Streamer> streamer{new Streamer{i1, i6, numItems}};

	mdf::Mdf<Drainer> engine{g, tn, unique_ptr<Drainer>{new Drainer}};

	streamer = engine.Start(move(streamer));

	} catch (std::exception& e) {
		cout << e.what() << endl;
		return -1;
	}

	return 0;
}

