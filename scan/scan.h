#include "parallel.h"

using namespace parlay;

// A serial implementation for checking correctness.
//
// Work = O(n)
// Depth = O(n)
template <class T, class F>
T scan_inplace_serial(T *A, size_t n, const F& f, T id) {
  T cur = id;
  for (size_t i=0; i<n; ++i) {
    T next = f(cur, A[i]);
    A[i] = cur;
    cur = next;
  }
  return cur;
}

// Parallel in-place prefix sums. Your implementation can allocate and
// use an extra n*sizeof(T) bytes of memory.
//
// The work/depth bounds of your implementation should be:
// Work = O(n)
// Depth = O(\log(n))
template <class T, class F>
T scan_inplace(T *A, size_t n, const F& f, T id) {
  T* L = (T*) malloc(n * sizeof(T));
  size_t sum = scan_up(A, n, L, f);
  scan_down(A, n, L, f, id);
  free(L);
  return sum;  // TODO
}

template <class T, class F>
T scan_up(T *A, size_t n, T *L, const F& f) {

  if (n == 1) {
    return A[0];
  }
  auto middle = n / 2 + (n % 2 != 0);
  T l, r;
  auto f1 = [&]()
  { l = scan_up(A, middle, L, f); };
  auto f2 = [&]()
  { r = scan_up(A + middle, n - middle, L + middle, f); };
  par_do(f1, f2);
  L[middle - 1] = l;
  return f(l, r);
}

  //T v1, v2;
  //auto f1 = [&]()
  //{ v1 = granular_reduce(A, n / 2, threshold); };
  //auto f2 = [&]()
  //{ v2 = granular_reduce(A + n / 2, n - n / 2, threshold); };
  //par_do(f1, f2);
  //return v1 + v2;

template <class T, class F>
void scan_down(T *R, size_t n, T *L, const F& f, T s) {
  if (n == 1) {
    R[0] = s;
    return;
  }
  else {
    auto middle = n / 2 + (n % 2 != 0);
    auto f_call = f(s, L[middle - 1]);
    auto f1 = [&]()
    { scan_down(R, middle, L, f, s); };
    auto f2 = [&]()
    { scan_down(R + middle, n - middle, L + middle, f, f_call); };
    par_do(f1, f2);
    return;
  }
}
