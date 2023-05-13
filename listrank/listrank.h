#include "parallel.h"
#include "random.h"

// For timing parts of your code.
#include "get_time.h"

// For computing sqrt(n)
#include <math.h>

using namespace parlay;

// Some helpful utilities
namespace {

// returns the log base 2 rounded up (works on ints or longs or unsigned
// versions)
template <class T>
size_t log2_up(T i) {
  assert(i > 0);
  size_t a = 0;
  T b = i - 1;
  while (b > 0) {
    b = b >> 1;
    a++;
  }
  return a;
}

}  // namespace


struct ListNode {
  ListNode* next;
  size_t rank;
  ListNode(ListNode* next) : next(next), rank(std::numeric_limits<size_t>::max()) {}
};

// Serial List Ranking. The rank of a node is its distance from the
// tail of the list. The tail is the node with `next` field nullptr.
//
// The work/depth bounds are:
// Work = O(n)
// Depth = O(n)
void SerialListRanking(ListNode* head) {
  size_t ctr = 0;
  ListNode* save = head;
  while (head != nullptr) {
    head = head->next;
    ++ctr;
  }
  head = save;
  --ctr;  // last node is distance 0
  while (head != nullptr) {
    head->rank = ctr;
    head = head->next;
    --ctr;
  }
}

// Wyllie's List Ranking. Based on pointer-jumping.
//
// The work/depth bounds of your implementation should be:
// Work = O(n*\log(n))
// Depth = O(\log^2(n))
void WyllieListRanking(ListNode* L, size_t n) {
  size_t *D = (size_t*) malloc(n * sizeof(size_t));
  ListNode** successors = (ListNode**) malloc(n * sizeof(ListNode*));
  size_t *D_prime = (size_t*) malloc(n * sizeof(size_t));
  ListNode** successors_prime = (ListNode**) malloc(n * sizeof(ListNode*));

  auto parallel_block_1 = [&](size_t x) 
  {
    successors[x] = L[x].next;
    successors_prime[x] = successors[x];
    if (successors[x] == nullptr) { //TODO
      D[x] = 0;
    }
    else {
      D[x] = 1;
    }
    L[x].rank = x;
  };

  parallel_for(0, n, parallel_block_1);

  auto parallel_block_2 = [&](size_t x)
  {
    if (successors[x] == nullptr) {
      successors_prime[x] = successors[x];
      D_prime[x] = D[x];
    }
    else {
      D_prime[x] = D[x] + D[successors[x] -> rank];
      successors_prime[x] = successors[successors[x] -> rank];
    }
  };

  for (size_t k = 0; k < ceil(log2(n)); k++) {
    parallel_for(0, n, parallel_block_2);
    std::swap(successors, successors_prime);
    std::swap(D, D_prime);
  }

  auto parallel_block_3 = [&](size_t x) 
  {
    L[x].rank = D[x];
  };
  parallel_for(0, n, parallel_block_3);

  //free all memory
  free(D);
  free(successors);
  free(D_prime);
  free(successors_prime);

}

// Sampling-Based List Ranking
//
// The work/depth bounds of your implementation should be:
// Work = O(n) whp
// Depth = O(\sqrt(n)* \log(n)) whp
void SamplingBasedListRanking(ListNode* L, size_t n, long num_samples=-1, parlay::random r=parlay::random(0)) {
  
  if (num_samples == -1) {
    num_samples = sqrt(n);
  }


  //Array to keep track of what node we think is the head
  bool* head_tracking = (bool*) malloc(n * sizeof(bool));
  //Array to hold the samples
  int* samples = (int*) malloc(n * sizeof(int));

  //Initialize head_tracking and ranks
  for (size_t x = 0; x < n; x++) {
    L[x].rank = x;
    head_tracking[x] = false;
  }

  //Setup random generator of integers
  std::uniform_int_distribution<int> distribution(0, n - 1);
  parlay::random_generator generator;

  srand(time(nullptr));
  auto sample_generation_and_head = [&](size_t x) 
  {
    auto random_from_gen = generator[x];
    auto random_from_dist = distribution(random_from_gen);
    if (random_from_dist < num_samples) {
      samples[x] = x;
    }
    else {
      samples[x] = -1;
    }
    if (L[x].next == nullptr) {
      samples[x] = x;
    }
    else {
      head_tracking[L[x].next -> rank] = true;
    }
  };
  parallel_for(0, n, sample_generation_and_head);

  //Get that head index
  size_t head_ind = -1;
  auto find_head = [&](size_t x) {
    if (head_tracking[x] == false) {
      samples[x] = x;
      head_ind = x;
    }
  };
  parallel_for(0, n, find_head);

  //1) Linked List construction
  ListNode* linked_list = (ListNode*) malloc(n * sizeof(ListNode));
  auto build_linked_list = [&](size_t x) 
  {
    if (samples[x] >= 0) {
      ListNode *curr = L[x].next;
      if (curr != nullptr) {
        linked_list[samples[x]].rank++;
      }
      while (curr != nullptr && samples[curr -> rank] == -1) {
        linked_list[samples[x]].rank++;
        curr = curr->next;
      }
      if (curr != nullptr) {
        linked_list[samples[x]].next = &linked_list[samples[curr->rank]];
      }
    }
  };
  parallel_for(0, n, build_linked_list);

  //2) Run serial weighted list-ranking algorithm

  /*
  size_t ctr = 0;
  ListNode* save = head;
  while (head != nullptr) {
    head = head->next;
    ++ctr;
  }
  head = save;
  --ctr;  // last node is distance 0
  while (head != nullptr) {
    head->rank = ctr;
    head = head->next;
    --ctr;
  }
  */

  size_t ctr = 0;
  size_t temp;
  ListNode* save = &linked_list[samples[head_ind]];
  ListNode* head = save;
  while (head != nullptr) {
    ctr += head->rank;
    head = head->next;
  }
  head = save;
  while (head != nullptr) {
    temp = head->rank;
    head->rank = ctr;
    head = head->next;
    ctr -= temp;
  }

  //3) Assign ranks for non-sampled nodes
  auto non_sampled_rank_assign = [&](size_t x) 
  {
    if (samples[x] >= 0) {
      size_t curr_rank = linked_list[samples[x]].rank;
      L[x].rank = curr_rank;
      ListNode *node = L[x].next;
      while (node != nullptr && samples[node -> rank] == -1) {
        curr_rank -= 1;
        L[node->rank].rank = curr_rank;
        node = node->next;
      }
    }
  };
  parallel_for(0, n, non_sampled_rank_assign);

  //free allocated mem
  free(linked_list);
  free(samples);
  free(head_tracking);
}

