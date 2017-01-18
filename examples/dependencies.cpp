/***********************************************

   Distributed Systems: Paradigms and models
   2015/2016 Final project source code
   Micro MDF
   Author: Andrea Maggiordomo

************************************************/

/*
 * Mdf example (side effect instructions)
 * This example uses mdf to compute a stream of matrix vector multiplications
 * The graph nodes are a simple array of functions that compute a portion of
 * the output vectors (that is, each node deals with a contiguous subset of rows
 * and multiplies each row with the input vector).
 *
 */

#include <cmath>

#include <utility>
#include <sstream>
#include <random>
#include <chrono>

#include "../mdf/Mdf.hpp"

using namespace std;
using namespace std::chrono;

struct cleanup { double *mat, *vec, *out; size_t dim; };

class Drainer {
	
public:

	void operator()(mdf::TokenHandle token)
	{
		mdf::ValueHandle<struct cleanup> result = dynamic_pointer_cast<mdf::Value<struct cleanup>>(token);
		if (result)	{
			auto cleanup = result->GetValue();
			size_t dim = cleanup.dim;
 			for (unsigned i = 0; i < dim; ++i) {
				double prod = 0;
				for (unsigned j = 0; j < dim; ++j) {
					prod += sin(cleanup.mat[i*dim+j] * cleanup.vec[j]);
				}
				assert(sin(prod) == cleanup.out[i]);
			}
			delete[] cleanup.mat;
			delete[] cleanup.vec;
			delete[] cleanup.out;
		} else
			mdf::out.Println("Drainer: downcast failed.");
	}

};

class Streamer {

private:

	vector<mdf::NodeId> _cnodes;
	mdf::NodeId _sink;
	size_t _dim;
	const int _maxItems;
	int _numItems;

public:

	Streamer(vector<mdf::NodeId> cnodes, mdf::NodeId sink, size_t dim, int maxItems) : _cnodes{cnodes}, _sink{sink}, _dim{dim}, _maxItems{maxItems}, _numItems{0}
	{
		assert(_maxItems > 0);
	}

	vector<mdf::InputTokenContainer> Next()
	{
		vector<mdf::InputTokenContainer> input;
		if (_numItems++ < _maxItems) {
			double * mat = new double[_dim*_dim];
			double * vec = new double[_dim];
			double * out = new double[_dim];
			for (size_t k = 0; k < _dim*_dim; ++k)
				mat[k] = k/(double)_numItems;
			for (size_t k = 0; k < _dim; ++k)
				vec[k] = 1.0 + (k/(double) _numItems);

			size_t split = _dim / _cnodes.size();
			size_t residual = _dim % _cnodes.size();
			size_t totAssigned = 0;
			for (unsigned i = 0; i < _cnodes.size(); ++i) {	
				size_t assigned;
				if (residual > 0) {
					assigned = split+1;
					residual--;
				} else assigned = split;
				input.emplace_back(mdf::InputTokenContainer{_cnodes[i], "nrows", mdf::WrapValue<size_t>(assigned)});
				input.emplace_back(mdf::InputTokenContainer{_cnodes[i], "mat", mdf::WrapValue<double*>(mat + totAssigned * _dim)});
				input.emplace_back(mdf::InputTokenContainer{_cnodes[i], "vec", mdf::WrapValue<double*>(vec)});
				input.emplace_back(mdf::InputTokenContainer{_cnodes[i], "out", mdf::WrapValue<double*>(out + totAssigned)});
				totAssigned += assigned;
			}
			input.emplace_back(mdf::InputTokenContainer{_sink, "mat", mdf::WrapValue<double*>(mat)});
			input.emplace_back(mdf::InputTokenContainer{_sink, "vec", mdf::WrapValue<double*>(vec)});
			input.emplace_back(mdf::InputTokenContainer{_sink, "out", mdf::WrapValue<double*>(out)});
			input.emplace_back(mdf::InputTokenContainer{_sink, "dim", mdf::WrapValue<size_t>(_dim)});
		}

		return input;
	}

};

int main(int argc, char *argv[])
{
	try {
	
	size_t dim = (argc>1) ? stoul(argv[1]) : 10; 
	size_t tn = (argc>2) ? stoul(argv[2]) : 1; 
	int numItems = (argc>3) ? stoi(argv[3]) : 100;

	tn = min(tn, dim);

	mdf::out.Println("Streaming ", numItems, " items of dimension ", dim, ", running ", tn, " threads.");

	mdf::Graph g{};

	vector<mdf::NodeId> computeNodes;
	for (size_t i = 0; i < tn; ++i) {
		computeNodes.emplace_back(g.AddInstruction(
				[dim](double *mat, double *vec, double *out, size_t nrows) -> void* {
					for (size_t k = 0; k < nrows; ++k) {
						double prod = 0;
						for (size_t i = 0; i < dim; ++i)
							prod += sin(mat[k*dim+i] * vec[i]);
						out[k] = sin(prod);
					}
					return nullptr;
				},
				mdf::ParamDecl<double*>{"mat"},
				mdf::ParamDecl<double*>{"vec"},
				mdf::ParamDecl<double*>{"out"},
				mdf::ParamDecl<size_t>{"nrows"}));
	}

	mdf::NodeId sink = g.AddInstruction(
			[dim](double *mat, double *vec, double *out, size_t dim) -> struct cleanup {
				return {mat, vec, out, dim}; // Simply forward to the drainer
			},
			mdf::ParamDecl<double*>{"mat"},
			mdf::ParamDecl<double*>{"vec"},
			mdf::ParamDecl<double*>{"out"},
			mdf::ParamDecl<size_t>{"dim"});
		

	for (size_t i = 0; i < computeNodes.size(); ++i)
		g.DeclareDependency(computeNodes[i], sink);


	unique_ptr<Streamer> streamer{new Streamer{computeNodes, sink, dim, numItems}};

	mdf::Mdf<Drainer> engine{g, tn, unique_ptr<Drainer>{new Drainer}};

	streamer = engine.Start(move(streamer));

	} catch (std::exception& e) {
		cout << e.what() << endl;
		return -1;
	}

	return 0;
}

