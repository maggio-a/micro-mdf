/***********************************************

   Distributed Systems: Paradigms and models
   2015/2016 Final project source code
   Micro MDF
   Author: Andrea Maggiordomo

************************************************/

#include <fstream>
#include <vector>
#include <string>
#include <cmath>

#include "../mdf/Mdf.hpp"

using namespace std;

const int MAX_ITER = 10000;
const int SIZE = 1<<10;
const int BLOCK_SIZE = 1<<6;
const int N_LINES = 1<<2;

struct Color {
	unsigned char r;
	unsigned char g;
	unsigned char b;
};

class Histogram {

public:

	int maxVal;
	int *ptr;
	
	Histogram(int size) : maxVal{0}, ptr{new int[size]} { }
	~Histogram() { delete [] ptr; }

	void ToPPM(string name)
	{
		ofstream ofs(name + ".ppm", ios_base::binary | ios_base::out | ios_base::trunc);
		if (!ofs) {
			mdf::err.Println("Histogram.ToPPM(): Unable to open file ", name + ".ppm");
		} else {
			ofs << "P6 " << SIZE << " " << SIZE << " 255\n";
			for (int i = 0; i < SIZE*SIZE; ++i) {
				if (ptr[i] == MAX_ITER) {
					ofs << (unsigned char) 0 << (unsigned char) 0 << (unsigned char) 0;
				} else {
					float intensity = std::pow(1.0f - (ptr[i]/(float)maxVal), 8.0f);
					unsigned char r = intensity*128;
					unsigned char g = intensity*128;
					unsigned char b = intensity*255;
					Color c{r, g, b};
					ofs << c.r << c.g << c.b;
				}
			}
			ofs.close();
		}
	}

};

class Drainer {

private:

	Histogram *_hst;

public:

	Drainer(Histogram *hst) : _hst{hst} {}

	void operator()(mdf::TokenHandle token)
	{
		mdf::ValueHandle<int> result = dynamic_pointer_cast<mdf::Value<int>>(token);
		if (result)	{
			int iter = result->GetValue();
			if (iter > _hst->maxVal) _hst->maxVal = iter;
		} else
			mdf::out.Println("Drainer: downcast failed.");
	}
};

class Streamer {

private:

	vector<mdf::NodeId> _nodes;
	int *_hptr;
	int _index;

	const int nblocks = (SIZE/BLOCK_SIZE) * (SIZE/BLOCK_SIZE);
	const int blocks_per_side = SIZE/BLOCK_SIZE;

public:

	Streamer(vector<mdf::NodeId> nodes, int *hptr) : _nodes{nodes}, _hptr{hptr}, _index{0}
	{
		mdf::out.Println("Streamer will generate ", nblocks, " blocks");
	}

	vector<mdf::InputTokenContainer> Next()
	{
		vector<mdf::InputTokenContainer> input;
		if (_index < nblocks) {
			int x0 = (_index % blocks_per_side)*BLOCK_SIZE;
			int ybase = SIZE - 1 - (_index / blocks_per_side)*BLOCK_SIZE;
			for (int i = 0; i < BLOCK_SIZE/N_LINES; ++i) {
				input.emplace_back(mdf::InputTokenContainer{_nodes[i], "hst", mdf::WrapValue<int*>(_hptr)});
				input.emplace_back(mdf::InputTokenContainer{_nodes[i], "x0", mdf::WrapValue<int>(x0)});
				input.emplace_back(mdf::InputTokenContainer{_nodes[i], "y0", mdf::WrapValue<int>(ybase-i*N_LINES)});
			}
			++_index;
		}

		return input;
	}

};

int main(int argc, char *argv[])
{
	try {

	unsigned long tn = (argc>1) ? stoul(argv[1]) : 2;

	mdf::out.Println("Running ", tn, " threads.");

	double re0 = -0.74364396916876561516;
	double w = -0.74364381764268717490 - (-0.74364396916876561516);
	double im0 = 0.13182588262473313035;
	double h = 0.13182603415081157061 - (0.13182588262473313035);

	mdf::Graph g{};
	
	constexpr int nsplits = BLOCK_SIZE / N_LINES;

	vector<vector<mdf::NodeId>> stages;

	stages.emplace_back(vector<mdf::NodeId>{});
	stages[0].reserve(nsplits);

	for (int i = 0; i < nsplits; ++i) {
		auto id = g.AddInstruction(
				[re0, w, im0, h](int *hst, int x0, int y0) -> int {
					int maxIter = 0;
					for (int k = 0; k < N_LINES; ++k) {
						for (int j = 0; j < BLOCK_SIZE; ++j) {
							double reC = re0 + (x0+j)*w/SIZE;
							double imC = im0 + (y0-k)*h/SIZE;
							double re = 0.0, im = 0.0;
							int i = 0;
							while (i < MAX_ITER && re*re + im*im <= 4.0) {
								double tmpre = re*re - im*im + reC;
								im = 2.0*re*im + imC;
								re = tmpre;
								++i;
							}
							hst[(SIZE - 1 -(y0-k))*SIZE + (x0+j)] = i;
							if (maxIter < i) maxIter = i;
						}
					}
					return maxIter;
				},
				mdf::ParamDecl<int*>{"hst"},
				mdf::ParamDecl<int>{"x0"},
				mdf::ParamDecl<int>{"y0"});
		stages[0].push_back(id);
	}

	for (int s = nsplits >> 1, k = 1; s > 0; s >>= 1, k++) {
		stages.emplace_back(vector<mdf::NodeId>{});
		stages[k].reserve(s);
		for (int i = 0; i < s; ++i) {
			stages[k].push_back(g.AddInstruction(
					[](int a, int b) -> int { return a>b ? a : b; },
					mdf::ParamDecl<int>{"a"},
					mdf::ParamDecl<int>{"b"}));
			g.Connect(stages[k-1][i*2], stages[k][i], "a");
			g.Connect(stages[k-1][i*2+1], stages[k][i], "b");
		}
	}

	Histogram hst{SIZE*SIZE};

	unique_ptr<Streamer> streamer{new Streamer{stages[0], hst.ptr}};

	mdf::Mdf<Drainer> engine{g, tn, unique_ptr<Drainer>{new Drainer{&hst}}};

	streamer = engine.Start(move(streamer));

	hst.ToPPM("image");

	} catch (std::exception& e) {
		cout << e.what() << endl;
		return -1;
	}

	return 0;
}

